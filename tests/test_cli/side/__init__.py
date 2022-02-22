from statisfactory.session import BaseSession


@BaseSession.hook_post_init()
def foobar(sess):
    print("REGISTERING CUSTOM SESSION HOOK")
    return sess
