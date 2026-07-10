from app import logging, schemas
from app.config import Config
from app.metadata.EPGStationDB import EPGStationDBClient, EPGStationRecordedRecord


class EPGStationMetadataProvider:
    """
    EPGStation の MariaDB から録画番組メタデータを取得するプロバイダクラス
    MetadataAnalyzer から呼び出され、録画ファイルに対応する番組情報を EPGStation 側のデータベースから取得する
    プロバイダがメタデータを取得できなかった場合は None を返し、MetadataAnalyzer は元の TSInfoAnalyzer による解析にフォールバックする

    T2 段階では EPGStation 側の生レコード (EPGStationRecordedRecord) の取得までを実装している
    schemas.RecordedProgram へのフィールドマッピングは T3 で実装するため、命中してもまだ None を返す
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
        EPGStation の MariaDB から録画番組メタデータを取得する

        T2 段階では EPGStation 側の生レコードを取得し、命中をログ出力するのみにとどめる
        (フィールドマッピングは T3 で実装するため、命中時もまだ None を返して TSInfoAnalyzer にフォールバックさせる)
        また MetadataAnalyzer.analyze() と同じく同期コンテキストで実行されるため、
        非同期ドライバではなく PyMySQL (同期ドライバ) を用いた EPGStationDBClient を使用する

        Returns:
            schemas.RecordedProgram | None: T2 段階では常に None
                (取得した EPGStation 生レコードを schemas.RecordedProgram にマッピングするのは T3)
        """

        # EPGStation 機能が無効な場合はそもそも MetadataAnalyzer から呼び出されないが、念のためガードしておく
        if Config().epgstation_metadata.enabled is False:
            return None

        # EPGStation の MariaDB から、録画ファイルに対応する生レコードを取得する
        db_client = EPGStationDBClient()
        record: EPGStationRecordedRecord | None = db_client.findRecord(
            file_path = self.recorded_video.file_path,
            file_size = self.recorded_video.file_size,
        )

        # 命中した場合はデバッグログを出力する (マッピングは T3 で実装するため、まだ None を返す)
        if record is not None:
            logging.debug(
                f'{self.recorded_video.file_path}: Matched EPGStation record '
                f'(recorded.id={record.id}, name={record.name}).'
            )
            # TODO: T3 で schemas.RecordedProgram へのフィールドマッピングを実装する
            return None

        # 未命中 / DB 不可達時は None を返し、TSInfoAnalyzer による解析にフォールバックさせる
        return None