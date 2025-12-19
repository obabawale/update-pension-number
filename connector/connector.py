import xmlrpc.client
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def authenticate(host, db, username, password):
    common = xmlrpc.client.ServerProxy(f"{host}/xmlrpc/2/common", allow_none=True)
    uid = common.authenticate(db, username, password, {})
    models = xmlrpc.client.ServerProxy(f"{host}/xmlrpc/2/object", allow_none=True)
    print(
        f"Authenticated to Odoo successfully. UID: {uid},  Models: {models}, DB: {db}, Password: {password}."
    )
    return uid, models, db, password
