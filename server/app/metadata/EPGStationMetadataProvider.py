from app import schemas


class EPGStationMetadataProvider:
    """
    EPGStation の MariaDB から録画番組メタデータを取得するプロバイダクラス
    MetadataAnalyzer から呼び出され、録画ファイルに対応する番組情報を EPGStation 側のデータベースから取得する
    プロバイダがメタデータを取得できなかった場合は None を返し、MetadataAnalyzer は元の TSInfoAnalyzer による解析にフォールバックする

    現状は T1 (骨架) 段階であり、実際のデータベース接続・クエリロジックは未実装 (常に None を返す)
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

        T1 段階ではデータベース接続ロジックを実装していないため、常に None を返す
        (T2 で実際の MariaDB クエリによるメタデータ取得を実装する予定)

        Returns:
            schemas.RecordedProgram | None: 取得した録画番組情報を表すモデル
                (取得できなかった場合は None が返される)
        """

        # TODO: T2 で EPGStation の MariaDB からメタデータを取得する処理を実装する
        # 現状は骨架のため、常に None を返して元の TSInfoAnalyzer による解析にフォールバックさせる
        return None