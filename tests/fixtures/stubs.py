queue = b'default'

job = {
    'queue': queue,
    'id': b'2a5079e7-387b-492f-a81c-68aa55c194c8',
    'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
    'description': b'fixtures.some_calculation(3, 4, z=2)',
    'timeout': 180,
    'created_at': b'2016-04-05T22:40:35Z',
}

child_job = {
    'queue': queue,
    'id': b'56e6ba45-1aa3-4724-8c9f-51b7b0031cee',
    'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
    'description': b'fixtures.some_calculation(3, 4, z=2)',
    'timeout': 180,
    'created_at': b'2016-04-05T22:40:35Z',
    'dependency_id': b'2a5079e7-387b-492f-a81c-68aa55c194c8',
}

job_id = job['id']

job_data = job['data']

child_job_id = child_job['id']

job_exc_info = b"Exception('We are here')"
