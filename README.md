框架说明
======

-   构建docker容器
    ------
        sudo docker run -itd --name <ps_name> -p 8001:8001 -v <项目地址>:/opt <image_name> /bin/bash
    
-   部署环境
    ------
    -   #### python环境
        3.6+  （建议使用3.6.8）
    -   #### django项目依赖库
        未发现版本不兼容情况时，默认使用最新版本
        -   #####
                django==2.2.4
                django-cors-headers
                psycopg2-binary
                djangorestframework
                django-filter
                
        -   [ ] 若使用celery，需要安装以下依赖

                celery==4.3.0
                django-celery-beat
                flower
                redis
                tornado<6.0.0
                
        -   [ ] 若使用zombodb索引，需要安装以下依赖

                django-zombodb
                elasticsearch==6.3.1
                elasticsearch-dsl==6.3.1
        
        -   [ ] 其他选装库

                django-import-export  # admin后台导入导出工具
                django-pandas  # 可将django的queryset转成pandas的dataframe
                pandas
                numpy
                requests  # 更方便的web访问
                xlrd  # excel读取
                xlwt  # excel写入
                jieba  # 结巴分词器

-   python代码规范
    ------
    1.  #### 编码
        -   如无特殊情况, 文件一律使用 UTF-8 编码
        -   如无特殊情况, 文件头部必须加入#-\*-coding:utf-8-*-标识
    2.  #### 代码格式
        很多IDE有自动纠错功能，以下说明都可自动规范
        1.  ##### 缩进
            -   统一使用 4 个空格进行缩进
        2.  ##### 行宽
            -   每行代码尽量不超过 80 个字符(在特殊情况下可以略微超过 80 ，但最长不得超过 120)
        3.  ##### 空格
            -   在二元运算符两边各空一格[=, -, +=, ==, >, in, is not, and]
            -   函数的参数列表中，","之后要有空格
            -   函数的参数列表中，默认值等号两边不要添加空格
            -   左括号之后，右括号之前不要加多余的空格
            -   字典对象的左括号之前不要多余的空格
            -   不要为对齐赋值语句而使用的额外空格
        4.  ##### 空行
            -   模块级函数和类定义之间空两行；
            -   类成员函数之间空一行；
            
                    class A:
     
                        def __init__(self):
                            pass
                     
                        def hello(self):
                            pass
                     
                    def main():
                        pass 
            -   可以使用多个空行分隔多组相关的函数
            -   函数中可以使用空行分隔出逻辑相关的代码
    3.  #### 引号
        简单说，自然语言使用双引号，机器标示使用单引号，因此 代码里 多数应该使用 单引号

        -   **自然语言 使用双引号 "..."**
        
            例如错误信息；
        -   **机器标识 使用单引号 '...'**
        
            例如 dict 里的 key
        -   **正则表达式 使用原生的双引号 r"..."**
        
        -   **文档字符串 (docstring) 使用三个双引号 """......"""**
        
    4.  #### 命名规范
        1.  模块
            -   模块尽量使用小写命名，首字母保持小写，尽量不要用下划线(除非多个单词，且数量不多的情况)
        2.  类名
            -   类名使用驼峰(CamelCase)命名风格，首字母大写，私有类可用一个下划线开头
            -   将相关的类和顶级函数放在同一个模块里. 不像Java, 没必要限制一个类一个模块.
        3.  函数
            -   函数名一律小写，如有多个单词，用下划线隔开（蛇形）
            -   私有函数在函数前加一个下划线_
        4.  变量名
            -   变量名尽量小写, 如有多个单词，用下划线隔开（蛇形）
            -   常量采用全大写，如有多个单词，使用下划线隔开