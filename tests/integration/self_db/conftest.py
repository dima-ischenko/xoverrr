import pytest

from xoverrr.core import DataQualityChecker, InDatabaseChecker


@pytest.fixture(params=['pandas', 'indb'], ids=['pandas', 'indb'])
def make_self_db_checker(request):
    def _make(engine, **kwargs):
        if request.param == 'pandas':
            return DataQualityChecker(
                source_engine=engine, target_engine=engine, **kwargs
            )
        return InDatabaseChecker(engine, **kwargs)

    return _make
