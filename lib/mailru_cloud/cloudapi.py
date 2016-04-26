# -*- coding: utf-8 -*-
import requests

USER_AGENT = 'User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.108 Safari/537.36'


class NotFoundError(Exception):
    pass


class FileExistsError(Exception):
    pass


class Cloud(object):
    max_download_zip_size = 1000000000
    download_prefix = '.'

    def __init__(self, login, password):
        self.session = requests.Session()
        self.authenticate(login, password)
        self.csrf_token = self.get_csrf_token()

    def authenticate(self, login, password):
        response = self.session.post('https://auth.mail.ru/cgi-bin/auth?lang=ru_RU&from=authpopup', data={
            'page': 'https://cloud.mail.ru/?from=promo',
            'FailPage': '',
            'Domain': 'mail.ru',
            'Login': login,
            'Password': password,
            'new_auth_form': '1',
            'saveauth': '1'})
        assert response.status_code == 200
        assert response.url == 'https://cloud.mail.ru/?from=promo&from=authpopup'

    def get_csrf_token(self):
        response = self.session.post('https://cloud.mail.ru/api/v2/tokens/csrf', data={'api': '2'})
        jresp = response.json()
        assert response.status_code == 200, response.status_code
        token = jresp['body']['token']
        assert token
        return token

    def api_tokens_download(self):
        response = self.session.post('https://cloud.mail.ru/api/v2/tokens/download',
                                     data={'api': '2', 'token': self.csrf_token})
        assert response.status_code == 200, response.status_code
        return response.json()['body']['token']

    _servers = None

    @property
    def servers(self):
        if self._servers is None:
            dispatcher_response = self.api_dispatcher()
            servers = {}
            for k, v in dispatcher_response.items():
                servers[k] = v[0]['url']
            self._servers = servers
        return self._servers

    def api_folder(self, path):
        response = self.session.get('https://cloud.mail.ru/api/v2/folder',
                                    params={'token': self.csrf_token, 'home': path})
        assert response.status_code in (200, 400), response.status_code
        jresp = response.json()
        if response.status_code == 200:
            assert jresp['body']['kind'] == 'folder'
            return jresp['body']
        else:
            assert jresp['body']['home']['error'] == 'not_exists'
            raise NotFoundError

    def api_file(self, path):
        response = self.session.get('https://cloud.mail.ru/api/v2/file',
                                    params={'token': self.csrf_token, 'home': path})
        assert response.status_code in (200, 404), response.status_code
        jresp = response.json()
        if response.status_code == 200:
            return jresp['body']
        else:
            assert jresp['body']['home']['error'] == 'not_exists'
            raise NotFoundError

    def api_dispatcher(self):
        response = self.session.get('https://cloud.mail.ru/api/v2/dispatcher', params={'token': self.csrf_token})
        assert response.status_code == 200
        return response.json()['body']

    def api_zip(self, paths):
        if not hasattr(paths, '__iter__'):
            paths = [paths]
        home_list = []
        for path in paths:
            path = path.encode('utf-8')
            home_list.append('"%s"' % path)
        home_list = '[%s]' % ','.join(home_list)
        response = self.session.post('https://cloud.mail.ru/api/v2/zip', data={
            'home_list': home_list,
            'name': self.download_prefix,
            'cp866': 'false',
            'api': '2',
            'token': self.csrf_token
        })
        assert response.status_code in (200, 400), response.status_code
        jresp = response.json()
        if response.status_code == 200:
            return jresp['body']
        else:
            raise NotFoundError

    def upload_blob(self, s):
        url = self.servers['upload']
        response = self.session.post(url, params={'cloud_domain': '2'}, files=[('file', ('filename', s))])
        assert response.status_code == 200, response.status_code
        result = response.text.split(';')
        assert len(result) == 2
        return {'hash': result[0], 'size': int(result[1])}

    def api_file_add(self, path, blob, conflict='strict'):
        """
            returns added file name, can be different if file with specified name exists
        """
        response = self.session.post('https://cloud.mail.ru/api/v2/file/add', data={
            'home': path,
            'hash': blob['hash'],
            'size': blob['size'],
            'conflict': conflict,
            'api': '2',
            'token': self.csrf_token
        })
        assert response.status_code in (200, 400), response.status_code
        jresp = response.json()
        if response.status_code == 200:
            return jresp['body']
        else:
            assert jresp['body']['home']['error'] == 'exists', jresp['body']['home']['error']
            raise FileExistsError

    def api_space(self):
        response = self.session.get('https://cloud.mail.ru/api/v2/user/space', params={'token': self.csrf_token})
        assert response.status_code == 200
        return response.json()['body']

    def api_file_remove(self, path):
        """
            removes files and folders
            does not raise exception if file doesnot exist
        """
        response = self.session.post('https://cloud.mail.ru/api/v2/file/remove', data={
            'home': path,
            'api': '2',
            'token': self.csrf_token
        })
        assert response.status_code == 200

    def api_folder_add(self, path, conflict='strict'):
        """
        conflict: strict or rename
        returns name of created folder
        """
        response = self.session.post('https://cloud.mail.ru/api/v2/folder/add', data={
            'home': path,
            'conflict': conflict,
            'api': '2',
            'token': self.csrf_token
        })
        assert response.status_code in (200, 400), response.status_code
        jresp = response.json()
        if response.status_code == 200:
            return jresp['body']
        else:
            assert jresp['body']['home']['error'] == 'exists'
            raise FileExistsError

    def api_file_rename(self, path, new_name, conflict='strict'):
        """
            Renanme file or folder

            new_name: without path

            conflict: rename or strict

            returns: new name, can differ from specified if already exists
        """
        response = self.session.post('https://cloud.mail.ru/api/v2/file/rename', data={
            'home': path,
            'name': new_name,
            'conflict': conflict,
            'api': '2',
            'token': self.csrf_token
        })
        assert response.status_code in (200, 400), response.status_code
        jresp = response.json()
        if response.status_code == 200:
            return jresp['body']
        else:
            assert jresp['body']['home']['error'] in ('not_exists', 'exists', 'invalid')
            if jresp['body']['home']['error'] == 'not_exists':
                raise NotFoundError
            else:
                raise FileExistsError

    def file_exists(self, path):
        """
            returns: False, file, folder
        """
        try:
            return self.api_file(path)['type']
        except NotFoundError:
            return False

    def upload_file(self, path, s):
        blob = self.upload_blob(s)
        self.api_file_add(path, blob)

    def get_file_reader(self, path):
        url = self.servers['get'][:-1] + path
        response = self.session.get(url, stream=True)
        assert response.status_code == 200, response.status_code
        return response.raw


if __name__ == '__main__':
    import time


    cloud = Cloud(login, password)

    f = open('/home/w/tmp/test')
    print cloud.upload_blob(f.read())
    # f = cloud.get_file_reader(u'/Полет.mp4')
    # f = cloud.get_file_reader(u'/Берег.jpg')
    # size = 0
    # while True:
    #     s = f.read(4096)
    #     size += len(s)
    #     if not s:
    #         break
    #     print '\r', size,

    # s = StringIO('a' * 10000000)
    # t = time.time()
    # blob = cloud.upload_blob(s)
    # cloud.api_file_add('/10m', blob)
    # print (time.time() - t)
    # cloud.download_file('/aaa/000.jpg', '/home/orlov/tmp')
    # print cloud.api_file_rename('/ye/1', '000.jpg')
    # auth = get_session_auth(login, password)
    # server_urls = dispatcher(auth)
    # blob = upload(open('/home/orlov/tmp/tip.html'), server_urls['upload'][0]['url'], auth)
    # assert os.path.getsize('/home/orlov/tmp/tip.html') == blob['size']
    # print file_add(u'/Новая папк/test', blob, auth)

    # get_download_token(auth)
    # file_data = get_file(u'/000.jpg', server_urls['get'][0]['url'], auth)
    # print get_download_url([u'/Берег.jpg', u'Новая папка/BM.zip'], auth)
