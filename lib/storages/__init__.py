# coding: utf-8
import local
import mailru_cloud

classes = {
    'Local': local.Local,
    'MailruCloud': mailru_cloud.MailruCloud
}


def get_storage(class_name, kwargs):
    return classes[class_name](**kwargs)
