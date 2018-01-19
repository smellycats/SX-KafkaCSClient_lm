# -*- coding: utf-8 -*-
import re


def hphm2hpzl(hphm, hpys, hpzl):
    if hphm is None:
        return '99'
    if len(hphm) <= 2:
        return '99'
    if hpzl == '7' or hpzl == '07':
        return '07'
    if hpzl == '8' or hpzl == '88' or hpzl == '08':
        return '08'
    if re.match('^[0-9a-zA-Z]+$', hphm) and len(hphm) == 5 and hpys == 1:
        return '07'
    if hphm[-1] == '领':
        return '04'
    if hphm[1] == '使':
        return '03'
    if hphm[-1] == '港':
        return '26'
    if hphm[-1] == '澳':
        return '27'
    if hpys == 3:
        return '06'
    if hphm[-1] == '学':
        return '16'
    if hpys == 1:
        return '01'
    if hphm[-1] == '警':
        return '23'
    if hphm[:2] == 'WJ':
        return '31'
    if hphm[-1] == '挂':
        return '15'
    if hpys == 0:
        return '32'
    if hpys == 2:
        return '02'
    if hpys == 4:
        return '88'
    return '99'