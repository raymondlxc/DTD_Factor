from __future__ import division
from scipy.optimize import fsolve,leastsq
from scipy.stats import norm
from math import pow, e, log
import numpy as np
import pandas as pd
def KMV(EV,St,sigma_St,F,r):

    def f(x):
        #定义fsolve函数所需要的参数f

        Vt = float(x[0])
        sigma = float(x[1])
        dt = float(x[2])

        def N(y):
    #   定义标准正态分布的累积函数
            return norm.cdf(y)

        return [Vt/St*N(dt) - pow(e, -r)*(F/St)*N(dt - sigma) - 1,
                sigma*(Vt/St)*N(dt) - sigma_St,
                (log(Vt/F) + r + pow(sigma, 2)/2)- sigma*dt]
    x0 = [EV,0.5,1]
    result = fsolve(f,x0,epsfcn=0.5)
    result=[result[0],result[1],result[2],result[2]-result[1]]#DTD结果为dt-sigma
    return result




