import asyncio
from datetime import datetime
from typing import Literal

import ariblib.constants

from app import logging, schemas
from app.config import Config
from app.constants import JST
from app.metadata.EPGStationDB import EPGStationDBClient, EPGStationRecordedRecord
from app.metadata.TSInfoAnalyzer import TSInfoAnalyzer
from app.utils.TSInformation import TSInformation


class EPGStationMetadataProvider:
    """
    EPGStation の MariaDB から録画番組メタデータを取得するプロバイダクラス
    MetadataAnalyzer から呼び出され、録画ファイルに対応する番組情報を EPGStation 側のデータベースから取得する
    プロバイダがメタデータを取得できなかった場合は None を返し、MetadataAnalyzer は元の TSInfoAnalyzer による解析にフォールバックする

    命中時は EPGStation の生レコード (EPGStationRecordedRecord) を schemas.RecordedProgram にマッピングして返す
    (返値の形状は TSInfoAnalyzer.analyze() と一致するため、下游の MetadataAnalyzer/RecordedScanTask がそのまま消費できる)
    """

    def __init__(self, recorded_video: schemas.RecordedVideo) -> None:
        """
        EPGStation メタデータ取得プロバイダを初期化する

        Args:
            recorded_video (schemas.RecordedVideo): 録画ファイル情報を表すモデル
                (TSInfoAnalyzer.__init__ と同じく、解析対象の録画ファイル情報を受け取る)
        """

        self.recorded_video = recorded_video

    def analyze(self) -> schemas.RecordedProgram | None:
        """
        EPGStation の MariaDB から録画番組メタデータを取得し、schemas.RecordedProgram にマッピングして返す

        MetadataAnalyzer.analyze() と同じく同期コンテキスト (asyncio.to_thread() / ProcessPoolExecutor) で実行されるため、
        非同期ドライバではなく PyMySQL (同期ドライバ) を用いた EPGStationDBClient を使用する

        Returns:
            schemas.RecordedProgram | None: 命中した場合は EPGStation のメタデータをマッピングした録画番組情報。
                未命中 / DB 不可達 / マッピング時の例外 は None を返し、TSInfoAnalyzer による解析にフォールバックさせる。
                (EPGStation メタデータのバグで録画スキャン全体が落ちることはないよう、例外は全て握り潰す)
        """

        # EPGStation 機能が無効な場合はそもそも MetadataAnalyzer から呼び出されないが、念のためガードしておく
        if Config().epgstation_metadata.enabled is False:
            return None

        # EPGStation の MariaDB から、録画ファイルに対応する生レコードを取得し、schemas.RecordedProgram にマッピングする
        ## EPGStationDBClient 内部でも接続/クエリ例外は握り潰されるが、EPGStation 側のいかなる例外も
        ## 録画スキャン全体を落とさないよう、取レコードからマッピングまでを一つの try/except で包む
        try:
            # EPGStation の MariaDB から、録画ファイルに対応する生レコードを取得する
            db_client = EPGStationDBClient()
            record: EPGStationRecordedRecord | None = db_client.findRecord(
                file_path = self.recorded_video.file_path,
                file_size = self.recorded_video.file_size,
            )

            # 未命中 / DB 不可達時は None を返し、TSInfoAnalyzer による解析にフォールバックさせる (正常回退なので warning は出さない)
            if record is None:
                return None

            # 命中した場合はデバッグログを出力する
            logging.debug(
                f'{self.recorded_video.file_path}: Matched EPGStation record '
                f'(recorded.id={record.id}, name={record.name}).'
            )

            # 命中した生レコードを schemas.RecordedProgram にマッピングする
            ## マッピング処理は scan パスと B2 同期パスで共通利用できるよう staticmethod に切り出されている
            recorded_program = asyncio.run(self.mapRecordToProgram(record, self.recorded_video))
        except Exception as ex:
            # 取レコード/マッピング中に例外が発生した場合は録画スキャン全体を落とさないよう warning を出して None を返す
            ## (TSInfoAnalyzer にフォールバック)
            logging.warning(
                f'{self.recorded_video.file_path}: Failed to get/map EPGStation record to RecordedProgram. '
                f'Falling back to TSInfoAnalyzer.',
                exc_info = ex,
            )
            return None

        return recorded_program

    @staticmethod
    async def mapRecordToProgram(
        record: EPGStationRecordedRecord,
        recorded_video: schemas.RecordedVideo,
        analyze_recording_time: bool = True,
    ) -> schemas.RecordedProgram | None:
        """
        EPGStationRecordedRecord を schemas.RecordedProgram にマッピングする
        MetadataAnalyzer (scan パス) と RecordedScanTask.runEPGStationSync (B2 同期パス) の両方から共通利用される

        Args:
            record (EPGStationRecordedRecord): EPGStation の recorded / video_file テーブルから取得した生レコード
            recorded_video (schemas.RecordedVideo): 結果の RecordedProgram.recorded_video に設定する録画ファイル情報
                (scan パスでは ffprobe で得た実値、B2 同期パスでは AV 未解析の占位値を渡す)
            analyze_recording_time (bool): 録画開始/終了時刻を TS の TOT から解析するかどうか (デフォルト: True)
                scan パスでは True で従来通り、B2 同期パスでは False にしてファイル読み込みを伴わない純 DB 同期を実現する

        Returns:
            schemas.RecordedProgram | None: マッピング結果 (必須フィールドが取得できない場合は None)
        """

        # タイトルは必須フィールドのため、取得できなければマッピングを諦める (TSInfoAnalyzer にフォールバック)
        if record.name is None:
            logging.warning(f'{recorded_video.file_path}: EPGStation record has no name. Skipping.')
            return None

        # 番組開始時刻・番組終了時刻は必須フィールドのため、いずれか欠損時はマッピングを諦める
        if record.startAt is None or record.endAt is None:
            logging.warning(f'{recorded_video.file_path}: EPGStation record has no startAt/endAt. Skipping.')
            return None

        # EPGStation の startAt / endAt は Unix ミリ秒 (bigint)
        start_time = datetime.fromtimestamp(record.startAt / 1000, tz = JST)
        end_time = datetime.fromtimestamp(record.endAt / 1000, tz = JST)

        # 番組長 (秒) は (endAt - startAt) / 1000 で算出する
        ## EPGStation 側にも duration (ms) が存在するが、startAt/endAt の差分がより確実
        duration = (record.endAt - record.startAt) / 1000

        # network_id / service_id を channelId (bigint) から逆解決する
        ## 【仮定】EPGStation の channelId は Mirakurun の約束に従い channelId = networkId * 100000 + serviceId でエンコードされている
        ## この環境では実 DB に接続して検証できないため Bob が実データで検証する想定
        network_id: int | None = None
        service_id: int | None = None
        if record.channelId is not None:
            network_id = record.channelId // 100000
            service_id = record.channelId % 100000

        # event_id を programId から逆解決する
        ## 【仮定】Mirakurun の約束に従い event_id = programId % 100000 でエンコードされている
        event_id: int | None = None
        if record.programId is not None:
            event_id = record.programId % 100000

        # チャンネル情報を合成する (network_id / service_id が取得でき、かつ network_type が判別できる場合のみ)
        channel = await EPGStationMetadataProvider._buildChannel(recorded_video.file_path, network_id, service_id) \
            if (network_id is not None and service_id is not None) else None

        # ジャンル情報を構築する
        genres = EPGStationMetadataProvider._buildGenres(record)

        # 番組詳細情報 (detail) を構築する
        detail = EPGStationMetadataProvider._parseExtended(record.extended)

        # 録画開始時刻・録画終了時刻を TOT から解析して設定する (margins 精度向上のため)
        ## EPGStation DB には編成予定時刻しかないため、物理的な録画境界は録画 TS の TOT から取得する
        ## MetadataAnalyzer は EPGStation プロバイダ命中時には TSInfoAnalyzer を生成しないため、
        ## ここで自前で録画時刻を設定しておかないと下游の margins/is_partially_recorded 計算がスキップされる
        ## 解析失敗時はそのまま None (margins は安全にデフォルト 0 になる) となるよう try/except で包む
        ## B2 同期パス (analyze_recording_time=False) ではファイル読み込みを伴う TSInfoAnalyzer の生成自体を行わない
        if analyze_recording_time is True:
            try:
                recording_time = TSInfoAnalyzer(recorded_video).analyzeRecordingTime()
                if recording_time is not None:
                    recorded_video.recording_start_time = recording_time[0]
                    recorded_video.recording_end_time = recording_time[1]
            except Exception as ex:
                logging.debug(
                    f'{recorded_video.file_path}: Failed to analyze recording time from TS for EPGStation metadata.',
                    exc_info = ex,
                )

        # 録画番組情報を表すモデルを作成する (TSInfoAnalyzer.analyze() の返値と形状が一致するよう組み立てる)
        recorded_program = schemas.RecordedProgram(
            recorded_video = recorded_video,
            channel = channel,
            network_id = network_id,
            service_id = service_id,
            event_id = event_id,
            title = record.name,
            description = record.description or '',
            detail = detail,
            start_time = start_time,
            end_time = end_time,
            duration = duration,
            is_free = True,  # EPGStation の recorded テーブルに is_free に該当するカラムはないためデフォルト True
            genres = genres,
            # 必須フィールドのため作成日時・更新日時は適当に現在時刻を入れている
            # この値は参照されず、DB の値は別途自動生成される
            created_at = datetime.now(tz = JST),
            updated_at = datetime.now(tz = JST),
        )
        # 以下のフィールドは EPGStation から合理的に取得できないため、schemas.RecordedProgram のデフォルト値に任せる
        ## series_title, episode_number, subtitle (番組タイトル解析が必要), audio 種別/言語 (AudioComponentDescriptor が必要)

        return recorded_program

    @staticmethod
    async def _buildChannel(file_path: str, network_id: int, service_id: int) -> schemas.Channel | None:
        """
        network_id / service_id から schemas.Channel を合成する
        TSInfoAnalyzer.__analyzeSDTInformation() の 447-470 段と同じ枠組み・同じ TSInformation helper を使うが、
        TS から SDT/NIT を読むのではなく EPGStation の channelId 逆解決結果から直接構築する

        Args:
            file_path (str): ログ出力用の録画ファイルパス
            network_id (int): EPGStation の channelId から逆解決した network_id
            service_id (int): EPGStation の channelId から逆解決した service_id

        Returns:
            schemas.Channel | None: 合成したチャンネル情報 (network_type が判別できなかった場合は None)
        """

        # ネットワーク種別を取得
        channel_type: Literal['GR', 'BS', 'CS', 'CATV', 'SKY', 'BS4K', 'OTHER'] = TSInformation.getNetworkType(network_id)
        if channel_type == 'OTHER':
            logging.warning(
                f'{file_path}: Unknown network_id {network_id} from EPGStation channelId. '
                f'Skipping channel synthesis.'
            )
            return None

        # リモコン番号を取得
        ## 地デジ (GR) は NIT がないと取得できないため 0 に設定する (TSInfoAnalyzer の remocon_id 取得失敗時と同様)
        if channel_type == 'GR':
            remocon_id = 0
        else:
            remocon_id = TSInformation.calculateRemoconID(channel_type, service_id)

        # チャンネル番号を算出
        ## calculateChannelNumber は Channel テーブルを参照する非同期関数のため await する。呼び出し側は、B2 同期パス（主イベントループ）では await、scan パス（同期サブプロセス）では asyncio.run で駆動する
        channel_number = await TSInformation.calculateChannelNumber(
            channel_type,
            network_id,
            service_id,
            remocon_id,
        )

        # チャンネル ID を生成
        channel_id = f'NID{network_id}-SID{service_id:03d}'

        # 表示用チャンネルID = チャンネルタイプ(小文字)+チャンネル番号
        display_channel_id = channel_type.lower() + channel_number

        # チャンネル情報を表すモデルを作成
        ## EPGStation の recorded テーブルにはチャンネル名・TSID が存在しないため、
        ## name は占位 (NID/SID 文字列)・transport_stream_id は None とする
        ## is_watchable=False の場合、channels テーブルに実チャンネルが存在すれば下游でそちらが優先されるため、name は正確でなくてもよい
        channel = schemas.Channel(
            id = channel_id,
            display_channel_id = display_channel_id,
            network_id = network_id,
            service_id = service_id,
            transport_stream_id = None,  # EPGStation には TSID に該当するカラムがない
            remocon_id = remocon_id,
            channel_number = channel_number,
            type = channel_type,
            name = f'NID{network_id}-SID{service_id}',  # 占位: 実チャンネルが channels テーブルにあれば下游で上書きされる
        )

        # サブチャンネルかどうかを算出
        channel.is_subchannel = TSInformation.calculateIsSubchannel(channel.type, channel.service_id)

        # ラジオチャンネルにはなり得ない (録画ファイルのバリデーションの時点で映像と音声があることを確認している)
        channel.is_radiochannel = False

        # 録画ファイル内の情報として含まれているだけのチャンネルなので（現在視聴できるとは限らない）、is_watchable を False に設定
        ## もし視聴可能な場合はすでに channels テーブルにそのチャンネルのレコードが存在しているはずなので、そちらが優先される
        channel.is_watchable = False

        return channel

    @staticmethod
    def _buildGenres(record: EPGStationRecordedRecord) -> list[schemas.Genre]:
        """
        EPGStationRecordedRecord の genre1/subGenre1, genre2/subGenre2, genre3/subGenre3 を
        ariblib.constants.CONTENT_TYPE を使って schemas.Genre のリストにマッピングする

        ARIB 整数コード → (大分類名, {中分類コード: 中分類名}) の変換は TSInfoAnalyzer / ProgramsRouter と同源同規則。

        Args:
            record (EPGStationRecordedRecord): EPGStation の生レコード

        Returns:
            list[schemas.Genre]: マッピング結果 (未知のコード / KeyError は例外を投げずにスキップする)
        """

        genre_pairs: list[tuple[int | None, int | None]] = [
            (record.genre1, record.subGenre1),
            (record.genre2, record.subGenre2),
            (record.genre3, record.subGenre3),
        ]

        genres: list[schemas.Genre] = []
        for major_code, sub_code in genre_pairs:
            # 大分類コードが未設定ならスキップ
            if major_code is None:
                continue

            # ariblib.constants.CONTENT_TYPE で (大分類名, {中分類コード: 中分類名}) に変換
            ## 未知のコード / KeyError は例外を投げずにスキップする
            try:
                genre_tuple = ariblib.constants.CONTENT_TYPE[major_code]
            except (KeyError, IndexError, TypeError):
                continue

            major_name = str(genre_tuple[0])
            subgenre_dict = genre_tuple[1]

            # 中分類名を取得 (sub_code が None / 未定義なら「未定義」)
            if sub_code is not None:
                try:
                    middle_name = str(subgenre_dict.get(sub_code, '未定義'))
                except (AttributeError, TypeError):
                    middle_name = '未定義'
            else:
                middle_name = '未定義'

            # KonomiTV の表記揺れ統一: "／" → "・" (TSInfoAnalyzer.py:608 / ProgramsRouter と同規則)
            ## AGENTS.md の規則に従い、辞書リテラルではなく TypedDict のコンストラクタで型構造を明示する
            genre_dict = schemas.Genre(
                major = major_name.replace('／', '・'),
                middle = middle_name.replace('／', '・'),
            )

            # 「拡張」ジャンルは user_genre (user_nibble) から拡張情報を取得する必要があるが、
            ## EPGStation の recorded テーブルには user_nibble に該当するカラムがないため復元できず、スキップする
            ## (TSInfoAnalyzer では event.user_genre から取得しているが、ここでは相当する情報源がない)
            if genre_dict['major'] == '拡張':
                continue

            genres.append(genre_dict)

        return genres

    @staticmethod
    def _parseExtended(extended: str | None) -> dict[str, str]:
        """
        EPGStation の extended (拡張描述テキスト) を KonomiTV の detail dict (見出し→本文) に変換する

        EPGStation / EIT の extended は "◇見出し\n本文" 形式のテキストで格納されていることが多い。
        見出し区切り (◇) がない場合は全体を「番組内容」として扱う (TSInfoAnalyzer.__analyzeEITInformation の
        見出し空時のフォールバックと同様)。TSInformation.formatString() で表記を正規化する点も TSInfoAnalyzer に合わせる。

        Args:
            extended (str | None): EPGStation の recorded.extended

        Returns:
            dict[str, str]: 見出し→本文の辞書 (extended が None / 空の場合は空辞書)
        """

        detail: dict[str, str] = {}
        if extended is None:
            return detail

        # TSInfoAnalyzer.__analyzeEITInformation と同様に formatString() で表記を正規化する
        text = TSInformation.formatString(extended)

        if '◇' in text:
            # ◇ で区切られたブロックごとに見出しと本文を抽出する
            for block in text.split('◇'):
                block = block.strip('\r\n')
                if block == '':
                    continue
                # 最初の改行までが見出し、以降が本文
                parts = block.split('\n', 1)
                head = parts[0].strip(' \r\n')
                body = parts[1].strip() if len(parts) > 1 else ''
                # 見出しが空の場合は固定で「番組内容」とする (TSInfoAnalyzer と同様)
                if head == '':
                    head = '番組内容'
                # 見出しの衝突を防ぐため、必要ならタブ文字を付加する (TSInfoAnalyzer と同様)
                while head in detail:
                    head += '\t'
                detail[head] = body
        else:
            # 見出し区切りがない場合は全体を「番組内容」とする
            body = text.strip()
            if body != '':
                detail['番組内容'] = body

        return detail