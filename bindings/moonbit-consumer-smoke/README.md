# External native MoonBit SDK acceptance

A separate MoonBit module with its own `moon.mod`, compiled against the
unpublished HawDB module through `moon.work`. It links the native staticlib
and must emit `EXTERNAL_CONSUMER_PASS` on real SQL and transaction success.
This is local-source consumption, not registry distribution or ABI certification.
