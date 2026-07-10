from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pymysql
import pymysql.cursors

from app import logging
from app.config import Config


@dataclass
class EPGStationRecordedRecord:
    """
    EPGStation の MariaDB から取得した recorded / video_file テーブルの生レコードを表すデータクラス

    フィールドを schemas.RecordedProgram 等にマッピングする処理は T3 で実装する予定であり、
    このクラスはあくまで EPGStation 側の生の値をそのまま保持することを目的とする
    """

    # recorded テーブルの主要カラム
    id: int | None  # recorded.id
    programId: int | None
    channelId: int | None  # recorded.channelId は bigint (整数) のため int
    startAt: int | None
    endAt: int | None
    duration: int | None
    name: str | None
    halfWidthName: str | None
    description: str | None
    halfWidthDescription: str | None
    extended: str | None
    halfWidthExtended: str | None
    genre1: int | None
    subGenre1: int | None
    genre2: int | None
    subGenre2: int | None
    genre3: int | None
    subGenre3: int | None
    isRecording: int | None

    # video_file テーブルのカラム
    video_file_id: int | None  # video_file.id
    parentDirectoryName: str | None
    filePath: str | None
    size: int | None


class EPGStationDBClient:
    """
    EPGStation の MariaDB に対する読み取り専用アクセスクライアント
    MetadataAnalyzer.analyze() は同期コンテキスト (asyncio.to_thread() / ProcessPoolExecutor) で実行されるため、
    非同期ドライバではなく PyMySQL (同期ドライバ) を用いて実装している
    このクラスは SELECT のみを発行し、いかなる書き込みも行わない
    """

    # プロセス単位でコネクションを遅延生成して再利用する (録画ファイルスキャン時にファイルごとに呼び出されるため)
    ## コネクションが切れた場合は再接続を1回だけ試みる
    _connection: pymysql.connections.Connection | None = None

    # 取得するカラム (recorded / video_file 両方)
    ## video_file 側のカラムは AS で別名をつけ、EPGStationRecordedRecord のフィールド名と整合させる
    _SELECT_COLUMNS: str = (
        'r.id, r.programId, r.channelId, r.startAt, r.endAt, r.duration, '
        'r.name, r.halfWidthName, r.description, r.halfWidthDescription, '
        'r.extended, r.halfWidthExtended, '
        'r.genre1, r.subGenre1, r.genre2, r.subGenre2, r.genre3, r.subGenre3, '
        'r.isRecording, '
        'v.id AS video_file_id, v.parentDirectoryName, v.filePath, v.size'
    )

    def __init__(self) -> None:
        """
        EPGStation の MariaDB 読み取り専用クライアントを初期化する
        実際の DB 接続は初回クエリ時まで遅延される (モジュール import 時には接続しない)
        """

        # 接続設定は Config() から都度読み込む (遅延再接続時に最新の設定が反映されるようにする)
        self.db_config = Config().epgstation_metadata.db
        self.dir_map = Config().epgstation_metadata.dir_map

    @classmethod
    def _getConnection(cls, db_config: Any) -> pymysql.connections.Connection | None:
        """
        プロセス単位でキャッシュされた DB コネクションを取得する
        コネクションが未生成の場合は新規に生成する (読み取り専用・DictCursor・utf8mb4)

        Args:
            db_config (Any): EPGStation の MariaDB 接続設定 (_ServerSettingsEPGStationMetadataDB)

        Returns:
            pymysql.connections.Connection | None: DB コネクション (接続失敗時は None)
        """

        # 既存のコネクションがまだ有効であればそれを再利用する
        if cls._connection is not None:
            try:
                cls._connection.ping(reconnect=True)
                return cls._connection
            except Exception as ex:
                # コネクションが切れている場合は破棄して再接続を試みる
                logging.debug('EPGStation DB connection ping failed. Reconnecting...', exc_info=ex)
                try:
                    cls._connection.close()
                except Exception:
                    pass
                cls._connection = None

        # 新規に読み取り専用コネクションを生成する
        try:
            connection = pymysql.connect(
                host = db_config.host,
                port = db_config.port,
                user = db_config.user,
                password = db_config.password,
                database = db_config.database,
                charset = 'utf8mb4',
                connect_timeout = 5,
                cursorclass = pymysql.cursors.DictCursor,
            )
            cls._connection = connection
            return connection
        except Exception as ex:
            # DB が不可達な場合は録画視聴スキャンを止めないよう、warning だけ出して None を返す
            logging.warning(f'EPGStation DB connection failed: {type(ex).__name__}: {ex}')
            cls._connection = None
            return None

    def _executeQuery(self, query: str, params: tuple[Any, ...]) -> list[dict[str, Any]] | None:
        """
        指定したパラメータ付き SELECT クエリを実行し、結果の行リストを返す
        コネクションが切れていた場合は1回だけ再接続してリトライする

        Args:
            query (str): 実行する SQL (パラメータは全て %s プレースホルダを使用すること)
            params (tuple[Any, ...]): プレースホルダにバインドする値のタプル

        Returns:
            list[dict[str, Any]] | None: 結果行のリスト (接続失敗等で実行できなかった場合は None)
        """

        connection = self._getConnection(self.db_config)
        if connection is None:
            return None

        for attempt in range(2):  # 初回 + 再接続1回のリトライ
            try:
                with connection.cursor() as cursor:
                    cursor.execute(query, params)
                    rows: list[dict[str, Any]] = cursor.fetchall()
                return rows
            except Exception as ex:
                # 初回失敗時のみ再接続を試みてリトライする
                logging.debug(f'EPGStation DB query failed (attempt {attempt + 1}).', exc_info=ex)
                # 最後の試行での失敗時は、無駄な再接続を避けるためそのまま None を返す
                if attempt == 1:
                    return None
                # コネクションを破棄して再生成
                try:
                    connection.close()
                except Exception:
                    pass
                self.__class__._connection = None
                connection = self._getConnection(self.db_config)
                if connection is None:
                    return None
                # 2回目のループでリトライ
        return None

    def _buildRecord(self, row: dict[str, Any]) -> EPGStationRecordedRecord:
        """
        クエリ結果の1行 (dict) から EPGStationRecordedRecord を構築する

        Args:
            row (dict[str, Any]): DictCursor から取得した1行分の辞書

        Returns:
            EPGStationRecordedRecord: 構築された生レコード
        """

        return EPGStationRecordedRecord(
            id = row.get('id'),
            programId = row.get('programId'),
            channelId = row.get('channelId'),
            startAt = row.get('startAt'),
            endAt = row.get('endAt'),
            duration = row.get('duration'),
            name = row.get('name'),
            halfWidthName = row.get('halfWidthName'),
            description = row.get('description'),
            halfWidthDescription = row.get('halfWidthDescription'),
            extended = row.get('extended'),
            halfWidthExtended = row.get('halfWidthExtended'),
            genre1 = row.get('genre1'),
            subGenre1 = row.get('subGenre1'),
            genre2 = row.get('genre2'),
            subGenre2 = row.get('subGenre2'),
            genre3 = row.get('genre3'),
            subGenre3 = row.get('subGenre3'),
            isRecording = row.get('isRecording'),
            video_file_id = row.get('video_file_id'),
            parentDirectoryName = row.get('parentDirectoryName'),
            filePath = row.get('filePath'),
            size = row.get('size'),
        )

    def findRecord(self, file_path: str, file_size: int) -> EPGStationRecordedRecord | None:
        """
        録画ファイルのローカル絶対パスとファイルサイズから、対応する EPGStation 側のレコードを取得する

        マッチング戦略:
            1. 主マッチング: dir_map (KonomiTV ローカルディレクトリプレフィックス -> EPGStation parentDirectoryName) を用いて
               ローカルパスを (parentDirectoryName, 相対 filePath) に逆解決し、パラメータ付きクエリで検索する
            2. フォールバック: 主マッチングで取得できなかった場合、basename(filePath) が一致し、かつ v.size が一致する行を検索する
               (ファイルサイズは強い識別キーとして機能する)

        Args:
            file_path (str): 録画ファイルのローカル絶対パス
            file_size (int): 録画ファイルのバイト数

        Returns:
            EPGStationRecordedRecord | None: マッチした生レコード (マッチしなかった場合や DB 不可達時は None)
        """

        # ローカルのパス区切り文字を正規化して扱う (Windows のバックスラッシュにも対応)
        local_path = Path(file_path)
        local_name = local_path.name  # basename (ディレクトリパスを含まないファイル名)

        # 1. 主マッチング: dir_map を用いた (parentDirectoryName, filePath) による検索
        ## dir_map のうち、ローカルパスのプレフィックスとして一致する最初のエントリから逆解決を試みる
        for local_prefix_str, parent_dir_name in self.dir_map.items():
            local_prefix = Path(local_prefix_str)
            try:
                relative = local_path.relative_to(local_prefix)
            except ValueError:
                # プレフィックスとして一致しないエントリはスキップする
                continue
            # EPGStation の filePath 区切り文字は '/' であるため、相対パスを POSIX 形式に正規化する
            relative_str = relative.as_posix()

            # パラメータ付きクエリで (格納先ディレクトリ名, 相対ファイルパス) が一致するレコードを取得する
            query = (
                f'SELECT {self._SELECT_COLUMNS} '
                'FROM video_file v JOIN recorded r ON r.id = v.recordedId '
                'WHERE v.parentDirectoryName = %s AND v.filePath = %s'
            )
            rows = self._executeQuery(query, (parent_dir_name, relative_str))
            if rows is None:
                # DB 接続エラー等でクエリ自体が実行できなかった場合はフォールバックも意味がないので None を返す
                return None
            if len(rows) > 0:
                if len(rows) > 1:
                    logging.warning(
                        f'{file_path}: Multiple EPGStation video_file records matched '
                        f'(parentDirectoryName={parent_dir_name!r}, filePath={relative_str!r}). '
                        f'Using the first one as a possible match ambiguity.'
                    )
                return self._buildRecord(rows[0])

            # 1つの dir_map エントリでマッチしなかった場合は、別のエントリでマッチする可能性があるため継続する
            continue

        # 2. フォールバック: basename(filePath) と v.size による検索
        basename = local_name
        query = (
            f'SELECT {self._SELECT_COLUMNS} '
            'FROM video_file v JOIN recorded r ON r.id = v.recordedId '
            'WHERE v.size = %s AND SUBSTRING_INDEX(v.filePath, \'/\', -1) = %s'
        )
        rows = self._executeQuery(query, (file_size, basename))
        if rows is None:
            return None
        if len(rows) > 0:
            if len(rows) > 1:
                logging.warning(
                    f'{file_path}: Multiple EPGStation video_file records matched by basename + size '
                    f'(basename={basename!r}, size={file_size}). '
                    f'Using the first one as a possible match ambiguity.'
                )
            return self._buildRecord(rows[0])

        # いずれのマッチングでも取得できなかった
        return None