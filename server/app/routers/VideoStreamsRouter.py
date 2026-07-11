
import asyncio
import json
import pathlib
from concurrent.futures import ProcessPoolExecutor
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from fastapi.responses import Response
from sse_starlette.sse import EventSourceResponse

from app import logging
from app.metadata.MetadataAnalyzer import MetadataAnalyzer
from app.models.RecordedProgram import RecordedProgram
from app.models.RecordedVideo import RecordedVideo
from app.streams.StreamEncodingOptions import (
    SplitQualityAndEncodingOptions,
    StreamQualityWithOptions,
)
from app.streams.VideoStream import VideoStream
from app.utils import ShutdownProcessPoolExecutor

# ルーター
router = APIRouter(
    tags = ['Streams'],
    prefix = '/api/streams/video',
)

# video_id ごとのロックは意図的にクリアしない（total 数は録画件数で上限が付き、要素は極小の Lock のみでメモリは無視できる。release 直後の locked()==False による解放レースを避けるため）
_file_locks: dict[int, asyncio.Lock] = {}
_file_locks_dict_lock: asyncio.Lock = asyncio.Lock()


async def ValidateVideoID(video_id: Annotated[int, Path(description='録画番組の ID 。')]) -> RecordedProgram:
    """ 録画番組 ID のバリデーション """

    # 指定された video_id が存在するか確認
    recorded_program = await RecordedProgram.filter(id=video_id).get_or_none() \
        .select_related('recorded_video') \
        .select_related('channel')
    if recorded_program is None:
        logging.error(f'[VideoStreamsRouter][ValidateVideoID] Specified video_id was not found. [video_id: {video_id}]')
        raise HTTPException(
            status_code = status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail = 'Specified video_id was not found',
        )

    if recorded_program.recorded_video.file_hash == '':
        async with _file_locks_dict_lock:
            lock = _file_locks.setdefault(video_id, asyncio.Lock())

        try:
            async with lock:
                # 二次確認
                current_video = await RecordedVideo.get_or_none(id=recorded_program.recorded_video.id)
                if current_video is not None and current_video.file_hash == '':
                    logging.info(f'[VideoStreamsRouter][ValidateVideoID] Lazy loading AV metadata for video_id: {video_id}')

                    analyzer = MetadataAnalyzer(pathlib.Path(current_video.file_path))
                    loop = asyncio.get_running_loop()
                    executor = ProcessPoolExecutor(max_workers=1)
                    should_wait_executor = True

                    try:
                        analyzed_video = await loop.run_in_executor(executor, analyzer.analyzeAVMetadataOnly)
                    except asyncio.CancelledError:
                        should_wait_executor = False
                        await ShutdownProcessPoolExecutor(executor, is_cancelled=True)
                        raise
                    except Exception as ex:
                        logging.error('[VideoStreamsRouter][ValidateVideoID] MetadataAnalyzer threw an exception:', exc_info=ex)
                        analyzed_video = None
                    finally:
                        if should_wait_executor is True:
                            await ShutdownProcessPoolExecutor(executor, is_cancelled=False)

                    if analyzed_video is None:
                        current_video.status = 'AnalysisFailed'
                        await current_video.save()

                        logging.error(f'[VideoStreamsRouter][ValidateVideoID] Failed to analyze AV metadata. [video_id: {video_id}]')
                        raise HTTPException(
                            status_code = status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail = 'Failed to analyze AV metadata',
                        )

                    current_video.file_hash = analyzed_video.file_hash
                    current_video.file_size = analyzed_video.file_size
                    current_video.file_created_at = analyzed_video.file_created_at
                    current_video.file_modified_at = analyzed_video.file_modified_at
                    current_video.duration = analyzed_video.duration
                    current_video.container_format = analyzed_video.container_format
                    current_video.video_codec = analyzed_video.video_codec
                    current_video.video_codec_profile = analyzed_video.video_codec_profile
                    current_video.video_scan_type = analyzed_video.video_scan_type
                    current_video.video_frame_rate = analyzed_video.video_frame_rate
                    current_video.video_resolution_width = analyzed_video.video_resolution_width
                    current_video.video_resolution_height = analyzed_video.video_resolution_height
                    current_video.has_video_stream_changes = analyzed_video.has_video_stream_changes
                    current_video.primary_audio_codec = analyzed_video.primary_audio_codec
                    current_video.primary_audio_channel = analyzed_video.primary_audio_channel
                    current_video.primary_audio_sampling_rate = analyzed_video.primary_audio_sampling_rate
                    current_video.secondary_audio_codec = analyzed_video.secondary_audio_codec
                    current_video.secondary_audio_channel = analyzed_video.secondary_audio_channel
                    current_video.secondary_audio_sampling_rate = analyzed_video.secondary_audio_sampling_rate

                    await current_video.save()

                # 本リクエストで解析を実行したかに関わらず、最新の recorded_program を再取得して返す (古いデータによる競合を修正)
                recorded_program = await RecordedProgram.filter(id=video_id).get_or_none() \
                    .select_related('recorded_video') \
                    .select_related('channel')
                if recorded_program is None:
                    raise HTTPException(
                        status_code = status.HTTP_422_UNPROCESSABLE_ENTITY,
                        detail = 'Recorded program not found after AV analysis',
                    )

        except HTTPException:
            raise
        except asyncio.CancelledError:
            raise
        except Exception as ex:
            logging.error('[VideoStreamsRouter][ValidateVideoID] Unexpected error during lazy loading AV metadata:', exc_info=ex)
            raise HTTPException(
                status_code = status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail = 'Failed to analyze AV metadata due to an unexpected error',
            )

    return recorded_program


async def ValidateQuality(quality: Annotated[str, Path(description='映像の品質。ex: 1080p')]) -> StreamQualityWithOptions:
    """ 映像の品質のバリデーション """

    # 指定された品質が存在するか確認
    ## 品質の指定に -10bit や -24fps が付いていれば分解する
    stream_quality = SplitQualityAndEncodingOptions(quality)
    if stream_quality is None:
        logging.error(f'[VideoStreamsRouter][ValidateQuality] Specified quality was not found. [quality: {quality}]')
        raise HTTPException(
            status_code = status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail = 'Specified quality was not found',
        )

    return stream_quality


@router.get(
    '/{video_id}/{quality}/playlist',
    summary = '録画番組 HLS M3U8 プレイリスト API',
    response_class = Response,
    responses = {
        status.HTTP_200_OK: {
            'description': '録画番組の HLS M3U8 プレイリスト。',
            'content': {'application/vnd.apple.mpegurl': {}},
        }
    }
)
async def VideoHLSPlaylistAPI(
    recorded_program: Annotated[RecordedProgram, Depends(ValidateVideoID)],
    stream_quality: Annotated[StreamQualityWithOptions, Depends(ValidateQuality)],
    session_id: Annotated[str, Query(description='セッション ID（クライアント側で適宜生成したランダム値を指定する）。')],
    cache_key: Annotated[str | None, Query(description='キャッシュ制御用のキー。')] = None,
):
    """
    指定された画質に対応する、録画番組のストリーミング用 HLS M3U8 プレイリストを返す。<br>
    この M3U8 プレイリストは仮想的なもので、すべてのセグメントデータがエンコード済みとは限らない。セグメントはリクエストされ次第随時生成される。
    """

    # 品質とオプション指定に対応する録画視聴セッションを作成または取得
    video_stream = VideoStream(
        session_id,
        recorded_program,
        stream_quality.quality,
        stream_quality.encoding_options,
        is_new_session_allowed = True,
    )

    # 仮想 HLS M3U8 プレイリストを取得
    virtual_playlist = video_stream.getVirtualPlaylist(cache_key)
    return Response(
        content = virtual_playlist,
        media_type = 'application/vnd.apple.mpegurl',
        headers = {
            'Cache-Control': 'max-age=0',
        },
    )


@router.get(
    '/{video_id}/{quality}/segment',
    summary = '録画番組 HLS セグメント API',
    response_class = Response,
    responses = {
        status.HTTP_200_OK: {
            'description': 'HLS セグメントとして分割された MPEG-TS データ。',
            'content': {'video/mp2t': {}},
        }
    }
)
async def VideoHLSSegmentAPI(
    recorded_program: Annotated[RecordedProgram, Depends(ValidateVideoID)],
    stream_quality: Annotated[StreamQualityWithOptions, Depends(ValidateQuality)],
    session_id: Annotated[str, Query(description='セッション ID（クライアント側で適宜生成したランダム値を指定する）。')],
    sequence: Annotated[int, Query(description='HLS セグメントの 0 スタートのシーケンス番号。')],
    cache_key: Annotated[str | None, Query(description='キャッシュ制御用のキー。')],
):
    """
    指定された画質に対応する、録画番組のストリーミング用 HLS セグメントを返す。<br>
    呼び出された時点でエンコードされていない場合は既存のエンコードタスクが終了され、<br>
    sequence の HLS セグメントが含まれる範囲から新たにエンコードタスクが開始される。
    """

    # 品質とオプション指定に対応する録画視聴セッションを取得
    video_stream = VideoStream(session_id, recorded_program, stream_quality.quality, stream_quality.encoding_options)

    # セグメントを取得（キャッシュキーはブラウザキャッシュ避けのための ID なので特に使わない）
    segment_data = await video_stream.getSegment(sequence)
    if segment_data is None:
        logging.error(
            f'{video_stream.log_prefix} Specified sequence segment was not found. '
            f'[sequence: {sequence}]'
        )
        raise HTTPException(
            status_code = status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail = 'Specified sequence segment was not found',
        )

    # 取得した MPEG-TS データを返す
    return Response(
        content = segment_data,
        media_type = 'video/mp2t',
        headers = {
            # キャッシュ有効期間を3時間に設定
            'Cache-Control': 'max-age=10800',
        },
    )


@router.get(
    '/{video_id}/{quality}/buffer',
    summary = '録画番組 HLS バッファ範囲 API',
    response_class = Response,
    responses = {
        status.HTTP_200_OK: {
            'description': '録画番組の HLS バッファ範囲が随時配信されるイベントストリーム。',
            'content': {'text/event-stream': {}},
        }
    }
)
async def VideoHLSBufferAPI(
    recorded_program: Annotated[RecordedProgram, Depends(ValidateVideoID)],
    stream_quality: Annotated[StreamQualityWithOptions, Depends(ValidateQuality)],
    session_id: Annotated[str, Query(description='セッション ID（クライアント側で適宜生成したランダム値を指定する）。')],
):
    """
    録画番組の HLS バッファ範囲を Server-Sent Events で随時配信する。

    イベントには、
    - バッファ範囲の更新を示す **buffer_range_update**
    の1種類がある。

    どのイベントでも配信される JSON 構造は同じ。<br>
    エンコードタスクが終了した場合は、接続を終了する。
    """

    # 品質とオプション指定に対応する録画視聴セッションを取得
    video_stream = VideoStream(session_id, recorded_program, stream_quality.quality, stream_quality.encoding_options)

    # バッファ範囲の変更を監視し、変更があればバッファ範囲をイベントストリームとして出力する
    async def generator():
        """イベントストリームを出力するジェネレーター"""

        # 初期値
        previous_buffer_range = video_stream.getBufferRange()

        # 初回接続時に必ず現在のバッファ範囲を返す
        yield {
            'event': 'buffer_range_update',  # buffer_range_update イベントを設定
            'data': json.dumps({
                'begin': previous_buffer_range[0],
                'end': previous_buffer_range[1],
            }),
        }

        while True:

            # 現在のバッファ範囲を取得
            buffer_range = video_stream.getBufferRange()

            # 以前の結果と異なっている場合のみレスポンスを返す
            if previous_buffer_range != buffer_range:
                logging.info(f'{video_stream.log_prefix} Buffer range updated. [begin: {buffer_range[0]}, end: {buffer_range[1]}]')
                yield {
                    'event': 'buffer_range_update',  # buffer_range_update イベントを設定
                    'data': json.dumps({
                        'begin': buffer_range[0],
                        'end': buffer_range[1],
                    }),
                }

                # 取得結果を保存
                previous_buffer_range = buffer_range

            # ビジーにならないように、0.1秒ごとにチェックする
            await asyncio.sleep(0.1)

    # EventSourceResponse でイベントストリームを配信する
    return EventSourceResponse(generator())


@router.put(
    '/{video_id}/{quality}/keep-alive',
    summary = '録画番組 HLS Keep-Alive API',
    status_code = status.HTTP_204_NO_CONTENT,
)
async def VideoHLSKeepAliveAPI(
    recorded_program: Annotated[RecordedProgram, Depends(ValidateVideoID)],
    stream_quality: Annotated[StreamQualityWithOptions, Depends(ValidateQuality)],
    session_id: Annotated[str, Query(description='セッション ID（クライアント側で適宜生成したランダム値を指定する）。')],
):
    """
    録画番組のストリーミング用 HLS セグメントの生成を継続するための API 。<br>
    ストリーミングセッションを維持するために、この API は録画番組の視聴を続けている間、定期的に呼び出さなければならない。<br>
    この API が定期的に呼び出されなくなった場合、一定時間後にストリーミング用 HLS セグメントの生成が停止され、メモリ上のデータが破棄される。
    """

    # 品質とオプション指定に対応する録画視聴セッションを取得
    video_stream = VideoStream(session_id, recorded_program, stream_quality.quality, stream_quality.encoding_options)

    # セッションのアクティブ状態を維持する
    video_stream.keepAlive()
