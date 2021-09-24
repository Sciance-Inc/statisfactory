from statisfactory import Craft, Session, Volatile


@Craft.make()
def craft_foo(param_1: int = 5) -> Volatile("foo_out"):
    return 2
