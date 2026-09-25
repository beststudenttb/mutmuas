"""Reception crypto: the secretary seals a bundle for one newcomer and signs it.

- newcomer: X25519 key pair made on its own machine; only the public key is sent.
- bundle:   ephemeral X25519 + HKDF-SHA256 + ChaCha20-Poly1305 (only the newcomer can open it);
- origin:   Ed25519 signature by the secretary; the public key is in this directory (secretary_ed25519.pub),
            so a newcomer can tell a real reply from a forged one.
"""
import base64, hashlib, json, os

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

INFO = b"mutmuas-reception-v1"
b64 = lambda b: base64.b64encode(b).decode()
unb64 = base64.b64decode
RAW = dict(encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw)


def fingerprint(pub_b64: str) -> str:
    return hashlib.sha256(unb64(pub_b64)).hexdigest()[:16]


def new_x25519() -> tuple[X25519PrivateKey, str]:
    k = X25519PrivateKey.generate()
    return k, b64(k.public_key().public_bytes(**RAW))


def _key(shared: bytes, eph_pub: bytes, rcpt_pub: bytes) -> bytes:
    return HKDF(hashes.SHA256(), 32, salt=None, info=INFO + eph_pub + rcpt_pub).derive(shared)


def _signed_part(env: dict) -> bytes:
    return json.dumps({k: env[k] for k in ("v", "invite", "eph", "nonce", "ct")}, sort_keys=True).encode()


def seal(files: dict[str, bytes], rcpt_pub_b64: str, invite: str, signer: Ed25519PrivateKey) -> dict:
    rcpt = unb64(rcpt_pub_b64)
    eph = X25519PrivateKey.generate(); eph_pub = eph.public_key().public_bytes(**RAW)
    key = _key(eph.exchange(X25519PublicKey.from_public_bytes(rcpt)), eph_pub, rcpt)
    nonce = os.urandom(12)
    plain = json.dumps({name: b64(data) for name, data in files.items()}).encode()
    env = {"v": 1, "invite": invite, "eph": b64(eph_pub), "nonce": b64(nonce),
           "ct": b64(ChaCha20Poly1305(key).encrypt(nonce, plain, invite.encode()))}
    env["sig"] = b64(signer.sign(_signed_part(env)))
    return env


def open_sealed(env: dict, my_key: X25519PrivateKey, secretary_pub_b64: str, invite: str) -> dict[str, bytes]:
    if env.get("v") != 1 or env.get("invite") != invite:
        raise ValueError("bundle is not for this invite")
    Ed25519PublicKey.from_public_bytes(unb64(secretary_pub_b64)).verify(unb64(env["sig"]), _signed_part(env))
    eph_pub = unb64(env["eph"]); my_pub = my_key.public_key().public_bytes(**RAW)
    key = _key(my_key.exchange(X25519PublicKey.from_public_bytes(eph_pub)), eph_pub, my_pub)
    plain = ChaCha20Poly1305(key).decrypt(unb64(env["nonce"]), unb64(env["ct"]), invite.encode())
    return {name: unb64(data) for name, data in json.loads(plain).items()}


def load_signer(path: str) -> Ed25519PrivateKey:
    return serialization.load_pem_private_key(open(path, "rb").read(), password=None)


def make_signer(path: str) -> str:
    k = Ed25519PrivateKey.generate()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as f:
        f.write(k.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                serialization.NoEncryption()))
    return b64(k.public_key().public_bytes(**RAW))
