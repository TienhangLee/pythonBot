from django.urls import path

from djanweb.views import listorders, listcustomers

urlpatterns = [
    # 添加如下的路由记录
    path('orders/', listorders),
    
    path('customers/', listcustomers),
]
