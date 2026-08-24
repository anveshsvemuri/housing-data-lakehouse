from housing_lakehouse.settings import PipelineSettings


def test_layer_paths_are_derived_from_data_root(tmp_path):
    settings = PipelineSettings(data_root=tmp_path)

    assert settings.bronze_path == tmp_path / "bronze"
    assert settings.silver_path == tmp_path / "silver"
    assert settings.gold_path == tmp_path / "gold"


def test_create_data_directories_is_idempotent(tmp_path):
    settings = PipelineSettings(data_root=tmp_path)

    settings.create_data_directories()
    settings.create_data_directories()

    assert all(
        path.is_dir()
        for path in (settings.bronze_path, settings.silver_path, settings.gold_path)
    )
