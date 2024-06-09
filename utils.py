from json import dump, load

from constants import Status


class IncrementalPrimaryKeyGenerator:
    def __init__(self, start: int = 1, inc_amt: int = 1) -> None:
        self._cur = start
        self.__inc_amt = inc_amt

    def use(self) -> int:
        amt = self._cur
        self._cur += self.__inc_amt
        return amt


pk_generators: dict[str, IncrementalPrimaryKeyGenerator] = {}


def make_fixture_model(model_ref, pk=None, fields={}):
    if not pk_generators.get(model_ref):
        pk_generators[model_ref] = IncrementalPrimaryKeyGenerator()

    if not pk:
        pk = pk_generators[model_ref].use()

    return {"model": model_ref, "pk": pk, "fields": fields}


def data_write(fname: str, data: list | dict):
    if not fname.endswith(".json"):
        fname = fname + ".json"

    with open("data/" + fname, "w") as f:
        dump(data, f, indent=4)


def json_load(fname: str):
    with open(fname, "r") as f:
        return load(f)


def data_load(fname: str):
    if not fname.endswith(".json"):
        fname = fname + ".json"

    return json_load("data/" + fname)


def get_is_active(data_dict: dict, status: str):
    if not status == Status.ON_PROD.value:
        return False
    else:
        return data_dict.get("is_active", True)
