from statisfactory import Context, Craft, Volatile

catalog = Context().catalog


@Craft.make(catalog)
def craft_spam(foo_out: Volatile) -> Volatile("spam_out"):
    return 3