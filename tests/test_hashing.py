from hearing_evidence.hashing import sha256_of


def test_known_value(tmp_path):
    p = tmp_path / "a.txt"
    p.write_bytes(b"abc")
    assert sha256_of(p) == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"


def test_changes_with_content(tmp_path):
    p = tmp_path / "a.txt"
    p.write_bytes(b"abc")
    first = sha256_of(p)
    p.write_bytes(b"abd")
    assert sha256_of(p) != first