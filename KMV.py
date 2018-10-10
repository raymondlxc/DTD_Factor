from __future__ import division
from scipy.optimize import fsolve,leastsq
from scipy.stats import norm
from math import pow, e, log
import numpy as np
import pandas as pd
def KMV(EV,St,sigma_St,F,r):

    def f(x):
        #定义fsolve函数所需要的参数f
        x0 = [EV,0.5,0.5]
        Vt = float(x[0])
        sigma = float(x[1])
        dt = float(x[2])

        def N(y):
    #   定义标准正态分布的累积函数
            return norm.cdf(y)

        return [Vt*N(dt) - pow(e, -r)*F*N(dt - sigma) - St,
                sigma*Vt*N(dt)/St - sigma_St,
                (log(Vt/F) + r + pow(sigma, 2)/2)- sigma*dt]

    result = fsolve(f,x0,epsfcn=0.5)
    return result




