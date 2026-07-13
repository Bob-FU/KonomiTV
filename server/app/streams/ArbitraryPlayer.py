from __future__ import annotations

import asyncio
import hashlib
import hmac
import itertools
import os
import pathlib
import threading
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass

from app import logging, schemas
from app.config import Config
from app.metadata.MetadataAnalyzer import MetadataAnalyzer
from app.metadata.TSInfoAnalyzer import TSInfoAnalyzer
from app.models.Channel import Channel
from app.models.RecordedProgram import RecordedProgram
from app.models.RecordedVideo import RecordedVideo
from app.utils import ShutdownProcessPoolExecutor
from app.utils.ProcessLimiter import ProcessLimiter


# 合成 ID は DB の自動採番 ID と衝突しない範囲から割り当てる
EPHEMERAL_ID_BASE = 2_000_000_000
# メモリ上の番組情報を保持する時間 (秒)
REGISTRY_TTL_SECONDS = 21_600
# メモリ上に保持する番組情報の最大件数
REGISTRY_MAX_ENTRIES = 256


@dataclass
class EphemeralEntry:
    """メモリ上だけに存在する録画番組情報"""

    recorded_program: RecordedProgram
    recorded_program_schema: schemas.RecordedProgram
    expires_at: float


_registry: dict[int, EphemeralEntry] = {}
_registry_lock = threading.Lock()
_id_counter = itertools.count(EPHEMERAL_ID_BASE)


def _analyze_arbitrary_file(real_path: str) -> schemas.RecordedProgram | None:
    """ProcessPoolExecutor 内で実行。analyze() に加え、MPEG-TS では EIT 非依存でチャンネル(SDT)/録画時刻(TOT)を補完する。"""

    analyzed = MetadataAnalyzer(pathlib.Path(real_path)).analyze()
    if analyzed is None:
        return None

    recorded_video = analyzed.recorded_video
    if recorded_video.container_format == 'MPEG-TS':
        # analyze() が EIT 失敗で channel を捨てた場合、SDT から復元
        if analyzed.channel is None:
            try:
                channel = TSInfoAnalyzer(recorded_video).analyzeChannel()
                if channel is not None:
                    analyzed.channel = channel
            except Exception:
                pass

        # 録画開始/終了時刻を TOT から補完 (EIT 非依存)
        if recorded_video.recording_start_time is None or recorded_video.recording_end_time is None:
            try:
                recording_time = TSInfoAnalyzer(recorded_video).analyzeRecordingTime()
                if recording_time is not None:
                    recorded_video.recording_start_time, recorded_video.recording_end_time = recording_time
            except Exception:
                pass

    return analyzed


def _remove_expired(now: float) -> None:
    """期限切れの番組情報を削除する"""

    expired_ids = [
        video_id for video_id, entry in _registry.items()
        if entry.expires_at <= now
    ]
    for video_id in expired_ids:
        del _registry[video_id]


def _get_entry(video_id: int) -> EphemeralEntry | None:
    """番組情報を取得し、ヒット時は有効期限を延長する"""

    if Config().arbitrary_player.enabled is False:
        return None

    now = time.monotonic()
    with _registry_lock:
        _remove_expired(now)
        entry = _registry.get(video_id)
        if entry is None:
            return None
        entry.expires_at = now + REGISTRY_TTL_SECONDS
        return entry


def register(
    program_orm: RecordedProgram,
    program_schema: schemas.RecordedProgram,
) -> int:
    """解析済みの番組情報をメモリ上の登録簿へ登録する"""

    now = time.monotonic()
    with _registry_lock:
        _remove_expired(now)
        while len(_registry) >= REGISTRY_MAX_ENTRIES:
            oldest_video_id = min(
                _registry,
                key=lambda video_id: _registry[video_id].expires_at,
            )
            del _registry[oldest_video_id]

        video_id = next(_id_counter)

        # ORM と API レスポンスの両方に同じ合成 ID を設定する
        program_orm.id = video_id
        program_orm.recorded_video.id = video_id
        program_orm.recorded_video.recorded_program_id = video_id
        program_schema.id = video_id
        program_schema.recorded_video.id = video_id

        _registry[video_id] = EphemeralEntry(
            recorded_program = program_orm,
            recorded_program_schema = program_schema,
            expires_at = now + REGISTRY_TTL_SECONDS,
        )

    return video_id


def get(video_id: int) -> RecordedProgram | None:
    """登録簿から ORM の録画番組情報を取得する"""

    entry = _get_entry(video_id)
    return entry.recorded_program if entry is not None else None


def get_schema(video_id: int) -> schemas.RecordedProgram | None:
    """登録簿から Pydantic の録画番組情報を取得する"""

    entry = _get_entry(video_id)
    return entry.recorded_program_schema if entry is not None else None


def verify_signature(path: str, provided_hash: str) -> bool:
    """ローカルファイルのパスに対する署名を検証する"""

    if Config().arbitrary_player.enabled is False or Config().arbitrary_player.salt == '':
        return False

    # realpath による正規化前の入力値を、そのまま署名対象として扱う
    expected = hmac.new(
        Config().arbitrary_player.salt.encode('utf-8'),
        path.encode('utf-8'),
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, provided_hash)


async def build_ephemeral_program(
    path: str,
) -> tuple[RecordedProgram, schemas.RecordedProgram] | None:
    """任意のローカルファイルを解析し、未保存の ORM とスキーマを構築する"""

    real_path = os.path.realpath(path)
    if os.path.isfile(real_path) is False:
        return None

    loop = asyncio.get_running_loop()

    async with ProcessLimiter.getSemaphore('ArbitraryPlayer'):
        executor = ProcessPoolExecutor(max_workers=1)
        should_wait_executor = True

        try:
            analyzed_program = await loop.run_in_executor(executor, _analyze_arbitrary_file, real_path)
        except asyncio.CancelledError:
            should_wait_executor = False
            await ShutdownProcessPoolExecutor(executor, is_cancelled=True)
            raise
        except Exception as ex:
            logging.error('[ArbitraryPlayer][build_ephemeral_program] MetadataAnalyzer threw an exception:', exc_info=ex)
            analyzed_program = None
        finally:
            if should_wait_executor is True:
                await ShutdownProcessPoolExecutor(executor, is_cancelled=False)

    if analyzed_program is None:
        return None

    recorded_video_schema = analyzed_program.recorded_video
    program = RecordedProgram()
    program.id = analyzed_program.id
    program.recording_start_margin = analyzed_program.recording_start_margin
    program.recording_end_margin = analyzed_program.recording_end_margin
    program.is_partially_recorded = analyzed_program.is_partially_recorded
    program.network_id = analyzed_program.network_id
    program.service_id = analyzed_program.service_id
    if analyzed_program.channel is not None:
        # RecordedScanTask.__populateChannelModelFromSchema と同等の写し (未保存 ORM)
        channel = Channel()
        channel.id = analyzed_program.channel.id
        channel.display_channel_id = analyzed_program.channel.display_channel_id
        channel.network_id = analyzed_program.channel.network_id
        channel.service_id = analyzed_program.channel.service_id
        channel.transport_stream_id = analyzed_program.channel.transport_stream_id
        channel.remocon_id = analyzed_program.channel.remocon_id
        channel.channel_number = analyzed_program.channel.channel_number
        channel.type = analyzed_program.channel.type
        channel.name = analyzed_program.channel.name
        channel.jikkyo_force = analyzed_program.channel.jikkyo_force
        channel.is_subchannel = analyzed_program.channel.is_subchannel
        channel.is_radiochannel = analyzed_program.channel.is_radiochannel
        channel.is_watchable = analyzed_program.channel.is_watchable
        program.channel = channel
        program.channel_id = channel.id
        # program 側の network_id/service_id も未設定なら channel から補完
        if program.network_id is None:
            program.network_id = channel.network_id
        if program.service_id is None:
            program.service_id = channel.service_id
    else:
        program.channel = None
        program.channel_id = None
    program.event_id = analyzed_program.event_id
    program.series = None
    program.series_id = None
    program.series_broadcast_period = None
    program.series_broadcast_period_id = None
    program.title = analyzed_program.title
    program.series_title = analyzed_program.series_title
    program.episode_number = analyzed_program.episode_number
    program.subtitle = analyzed_program.subtitle
    program.description = analyzed_program.description
    program.detail = analyzed_program.detail
    program.start_time = analyzed_program.start_time
    program.end_time = analyzed_program.end_time
    program.duration = analyzed_program.duration
    program.is_free = analyzed_program.is_free
    program.genres = analyzed_program.genres
    program.primary_audio_type = analyzed_program.primary_audio_type
    program.primary_audio_language = analyzed_program.primary_audio_language
    program.secondary_audio_type = analyzed_program.secondary_audio_type
    program.secondary_audio_language = analyzed_program.secondary_audio_language
    program.created_at = analyzed_program.created_at
    program.updated_at = analyzed_program.updated_at

    recorded_video = RecordedVideo()
    recorded_video.id = recorded_video_schema.id
    recorded_video.recorded_program_id = program.id
    recorded_video.status = recorded_video_schema.status
    recorded_video.file_path = str(recorded_video_schema.file_path)
    recorded_video.file_hash = recorded_video_schema.file_hash
    recorded_video.file_size = recorded_video_schema.file_size
    recorded_video.file_created_at = recorded_video_schema.file_created_at
    recorded_video.file_modified_at = recorded_video_schema.file_modified_at
    recorded_video.recording_start_time = recorded_video_schema.recording_start_time
    recorded_video.recording_end_time = recorded_video_schema.recording_end_time
    recorded_video.duration = recorded_video_schema.duration
    recorded_video.container_format = recorded_video_schema.container_format
    recorded_video.video_codec = recorded_video_schema.video_codec
    recorded_video.video_codec_profile = recorded_video_schema.video_codec_profile
    recorded_video.video_scan_type = recorded_video_schema.video_scan_type
    recorded_video.video_frame_rate = recorded_video_schema.video_frame_rate
    recorded_video.video_resolution_width = recorded_video_schema.video_resolution_width
    recorded_video.video_resolution_height = recorded_video_schema.video_resolution_height
    recorded_video.has_video_stream_changes = recorded_video_schema.has_video_stream_changes
    recorded_video.primary_audio_codec = recorded_video_schema.primary_audio_codec
    recorded_video.primary_audio_channel = recorded_video_schema.primary_audio_channel
    recorded_video.primary_audio_sampling_rate = recorded_video_schema.primary_audio_sampling_rate
    recorded_video.secondary_audio_codec = recorded_video_schema.secondary_audio_codec
    recorded_video.secondary_audio_channel = recorded_video_schema.secondary_audio_channel
    recorded_video.secondary_audio_sampling_rate = recorded_video_schema.secondary_audio_sampling_rate
    recorded_video.key_frames = []
    recorded_video.segment_map = []
    recorded_video.cm_sections = None
    recorded_video.thumbnail_info = None
    recorded_video.created_at = recorded_video_schema.created_at
    recorded_video.updated_at = recorded_video_schema.updated_at

    # 未保存の ORM 同士を明示的に双方向へ紐付ける
    ## recorded_video は逆参照 OneToOne（related_name）で読取専用プロパティ（setter なし）のため、
    ## select_related と同じく Tortoise 内部のキャッシュ属性 _recorded_video に直接代入する
    program._recorded_video = recorded_video
    recorded_video.recorded_program = program
    recorded_video.recorded_program_id = program.id

    return program, analyzed_program
