from statisfactory import Craft, BaseSession, Volatile


@Craft()
def craft_spam(foo_out: Volatile, *, param_1, param_2) -> Volatile("spam_out"):
    return 3
