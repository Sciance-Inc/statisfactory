from statisfactory import Session, Craft, Volatile

catalog = Session().catalog


@Craft.make(catalog)
def craft_foo(param_1: int = 5) -> Volatile("foo_out"):
    return 2
