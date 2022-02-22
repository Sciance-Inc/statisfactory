from statisfactory.session import BaseSession


@BaseSession.hook_post_init()
def foobar(sess):
    sess.side_only_flag = 1
    return sess
