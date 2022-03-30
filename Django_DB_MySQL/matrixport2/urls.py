from django.urls import path
from matrixport2.Matrix_baseTools import Tools, httpTools
from matrixport2.Matrix_updateTools import *

urlpatterns = [
    # 添加如下的路由记录

    path('fortest2/', httpTools.message_test),
    path('fortest3/', httpTools.message_test_HttpResponse),
    path('fortest4/', httpTools.message_test_JsonResponse),
    path('fortest5/', addequityValue),
    path('fortest6/', deleteEquityValue),
    path('fortest7/', del_equityValue),
    path('equityvalue/', show_equityValue),
    path('TRC20address/', show_trc20Address),
    path('ERC20address/', show_erc20Address),
    path('FILaddress/', show_filAddress),
]

