from statisfactory import Session, Craft, Volatile

catalog = Session().catalog


@Craft.make()
def craft_spam(foo_out: Volatile, param_1, param_2) -> Volatile("spam_out"):
    return 3
