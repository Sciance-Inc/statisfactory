from statisfactory import Craft, Volatile


@Craft()
def foo(val=2) -> Volatile("spam"):
    print(f"FOO REICEIVED PARAM VAL : {val}")
    return 1


@Craft()
def bar(spam: Volatile, val=2):
    print(f"BAR REICEIVED PARAM VAL : {val}")
