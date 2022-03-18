from django.urls import path
from fortest.testTools import Tools, httpTools
from fortest.DB_updateTest import *

urlpatterns = [
    # 添加如下的路由记录

    path('fortest2/', httpTools.message_test),
    path('fortest3/', httpTools.message_test_HttpResponse),
    path('fortest4/', httpTools.message_test_JsonResponse),
    path('fortest5/', addcryptoPrice),
    path('fortest6/', deletecryptoPrice),
    path('fortest7/', del_cryptoPrice),
    path('fortest8/', show_cryptoPrice),
]

