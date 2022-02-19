from statisfactory.session.base_session import BaseSession


class MySession(BaseSession):
    custom_session_flag = 1


@MySession.hook_post_init()
def set_second_value(sess: MySession) -> MySession:
    sess.custom_session_flag_2 = 1
    return sess
