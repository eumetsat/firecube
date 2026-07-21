# Copyright 2025-2026 EUMETSAT
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from firecube.core.credentials import Credentials  # pyright: ignore[reportMissingImports]


def test_equal_credentials_have_equal_fingerprint() -> None:
    left = Credentials(access_key="a", secret_key="b", session_token="c")
    right = Credentials(access_key="a", secret_key="b", session_token="c")

    assert left.fingerprint() == right.fingerprint()


def test_different_access_key_changes_fingerprint() -> None:
    left = Credentials(access_key="a", secret_key="b")
    right = Credentials(access_key="x", secret_key="b")

    assert left.fingerprint() != right.fingerprint()


def test_different_secret_key_changes_fingerprint() -> None:
    left = Credentials(access_key="a", secret_key="b")
    right = Credentials(access_key="a", secret_key="x")

    assert left.fingerprint() != right.fingerprint()


def test_different_session_token_changes_fingerprint() -> None:
    left = Credentials(access_key="a", secret_key="b", session_token="c")
    right = Credentials(access_key="a", secret_key="b", session_token="x")

    assert left.fingerprint() != right.fingerprint()


def test_repr_hides_raw_credential_values() -> None:
    creds = Credentials(access_key="AK", secret_key="SK", session_token="ST")

    text = repr(creds)

    assert "AK" not in text
    assert "SK" not in text
    assert "ST" not in text


def test_none_access_key_differs_from_real_value_in_fingerprint() -> None:
    anonymous = Credentials(access_key=None, secret_key="b")
    named = Credentials(access_key="a", secret_key="b")

    assert anonymous.fingerprint() != named.fingerprint()


def test_is_anonymous_requires_both_access_and_secret_to_be_none() -> None:
    assert Credentials(access_key=None, secret_key=None).is_anonymous() is True
    assert Credentials(access_key="a", secret_key=None).is_anonymous() is False
    assert Credentials(access_key=None, secret_key="b").is_anonymous() is False
    assert Credentials(access_key="a", secret_key="b").is_anonymous() is False
