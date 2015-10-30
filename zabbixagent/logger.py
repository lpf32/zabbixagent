# -*- coding: utf-8 -*-
__author__ = 'zhangpanpan'

from logging import getLogger
from inspect import currentframe

def get_logger():
    call_frame = currentframe().f_back
    name = call_frame.f_locals['self'].__module__

    return getLogger(name)