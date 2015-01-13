# Copyright (C) 2013 Hewlett-Packard Development Company, L.P.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""Tests for db purge."""

from datetime import datetime
from datetime import timedelta
import uuid

from cinder import context
from cinder import db
from cinder.db.sqlalchemy import api as db_api
from cinder.openstack.common import log as logging
from cinder import test

from oslo.db.sqlalchemy import utils as sqlalchemyutils


LOG = logging.getLogger(__name__)


class PurgeDeletedTest(test.TestCase):

    def setUp(self):
        super(PurgeDeletedTest, self).setUp()
        self.context = context.get_admin_context()
        self.engine = db_api.get_engine()
        self.session = db_api.get_session()
        self.conn = self.engine.connect()
        self.volumes = sqlalchemyutils.get_table(
            self.engine, "volumes")
        self.uuidstrs = []
        for unused in range(6):
            self.uuidstrs.append(uuid.uuid4().hex)

    def test_purge_deleted_rows(self):
        # Add 6 rows to table
        for uuidstr in self.uuidstrs:
            ins_stmt = self.volumes.insert().values(id=uuidstr)
            self.conn.execute(ins_stmt)
        # Set 4 of them deleted, 2 are 60 days ago, 2 are 20 days ago
        old = datetime.now() - timedelta(days=20)
        older = datetime.now() - timedelta(days=60)
        make_old = self.volumes.update().\
            where(self.volumes.c.id.in_(self.uuidstrs[1:3]))\
            .values(deleted_at=old)
        make_older = self.volumes.update().\
            where(self.volumes.c.id.in_(self.uuidstrs[4:6]))\
            .values(deleted_at=older)
        self.conn.execute(make_old)
        self.conn.execute(make_older)
        # Purge at 30 days old, should only delete 2 rows
        db.purge_deleted_rows(self.context, age=30)
        rows = self.session.query(self.volumes).count()
        # Verify that we only deleted 2
        self.assertEqual(rows, 4)
        # Purge at 10 days old now, should delete 2 more rows
        db.purge_deleted_rows(self.context, age=10)
        rows = self.session.query(self.volumes).count()
        # Verify that we only have 2 rows now
        self.assertEqual(rows, 2)
