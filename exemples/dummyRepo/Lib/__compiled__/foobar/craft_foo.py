from statisfactory import Session, Craft, Volatile


@Craft.make()
def craft_foo(param_1: int = 5) -> Volatile("foo_out"):
    return 2
