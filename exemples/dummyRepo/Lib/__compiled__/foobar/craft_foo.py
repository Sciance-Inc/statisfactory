from statisfactory import Context, Craft, Volatile

catalog = Context().catalog


@Craft.make(catalog)
def craft_foo() -> Volatile("foo_out"):
    return 2