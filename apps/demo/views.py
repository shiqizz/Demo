# -*- coding: utf-8 -*-
from base.permissions import permissions, IsOwnerOrReadOnly
from base.views import BaseAPIView
from rest_framework.response import Response
from rest_framework.renderers import TemplateHTMLRenderer
from rest_framework.generics import GenericAPIView, RetrieveAPIView
from rest_framework import mixins
from .serializers import (DemoModel,
                          DemoSerializer, )
from .forms import FileUploadForm


class DemoList(BaseAPIView, GenericAPIView):
    """
    列表
    """
    pagination_class = None
    permission_classes = (IsOwnerOrReadOnly,)
    serializer_class = DemoSerializer
    renderer_classes = [TemplateHTMLRenderer]

    def get_queryset(self):
        queryset = DemoModel.objects.all()
        return queryset

    def get(self, request, *args, **kwargs):
        context = {'form': FileUploadForm(), 'what': '数据上传'}
        return Response(context, template_name='demo/fileup.html')

    def post(self, request, *args, **kwargs):
        myform = FileUploadForm(request.POST, request.FILES)
        # 在这里可以添加筛选excel的机制
        if myform.is_valid():
            f = request.FILES['my_file']
            print(f)
        context = {'form': FileUploadForm(), 'what': '数据上传', 'upload_result': '上传成功'}
        return Response(context, template_name='demo/fileup.html')


class DemoDetail(BaseAPIView, mixins.RetrieveModelMixin, mixins.UpdateModelMixin, mixins.DestroyModelMixin, GenericAPIView):
    """
    详情
    """
    pagination_class = None
    permission_classes = (permissions.AllowAny,)
    serializer_class = DemoSerializer
    owner_field = 'user'
    lookup_field = 'pk'

    def get_queryset(self):
        user = self.request.user
        queryset = DemoModel.objects.filter(user=user)
        return queryset

    def _get(self, request, *args, **kwargs):
        res = self.retrieve(request, *args, **kwargs)
        self.result["data"] = res.data

    def _put(self, request, *args, **kwargs):
        res = self.partial_update(request, *args, **kwargs)
        self.result["data"] = res.data

    def _delete(self, request, *args, **kwargs):
        self.result = self.destroy(request, *args, **kwargs)
