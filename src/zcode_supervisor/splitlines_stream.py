"""Shared splitlines-exact streaming segmenter.

WP-A convergence (items 3+4): ``hub_server._split_segments`` /
``hub_server._scan_new`` and ``codex_driver._split_index_segments`` /
the inline scan in ``codex_driver._index_hints`` were two byte-equivalent
copies of the same P2.4-repair1 F-C engine (incremental utf-8 decode +
single-pass splitting on the full ``str.splitlines`` separator set,
scanning only NEW bytes per chunk — repair2 R1 linearity). They now
delegate to THIS module; the per-side I/O loops (hub: O_NOFOLLOW +
single-link admission + ``os.read``; hints: plain buffered ``open``)
stay in their modules because their admission and error contracts
differ, and both public behaviours (segment streams, ValueError
message shapes) are byte-identical to the pre-convergence state —
pinned by the P2.4 differential fuzz and the six-piece battery.

WP-A item 4 (EOF exactness): the shared EOF rule now reproduces
``str.splitlines`` EXACTLY, including a lone trailing ``\\r``:

* separator-terminated segments are always emitted;
* the unterminated tail is emitted only when non-empty (so ``'X\\n'``
  -> ``['X']``, ``'\\n'`` -> ``['']``, ``''`` -> ``[]``);
* a dangling trailing ``\\r`` at EOF IS a separator (``'X\\r'`` ->
  ``['X']``, ``'…\\x0b\\r'`` -> ``['…', '']``, ``'\\r\\r\\r'`` ->
  ``['', '', '']``) — the P2.4 repair3 R4/N3 exemption (one lost
  trailing empty segment when the sentinel ``\\n`` paired with the
  dangling ``\\r``) is gone; any divergence from the
  ``bytes.decode('utf-8', 'replace').splitlines()`` oracle is now a
  failure.

The consumer layers are neutral to the newly emitted empty segments:
the hub index scan skips blank lines (``not line.strip()``) and the
hints JSON gate drops them (``json.loads('')`` fails) — record
boundaries and all counters are unchanged.
"""

from __future__ import annotations

# The full str.splitlines separator set (P2.4-repair1 F-C): a plain
# text-mode line iteration only splits on \n/\r/\r\n; the old readers
# used str.splitlines(), so exotic separators keep the old record
# boundaries (and malformed classifications).
LINE_SEPARATORS = frozenset("\n\r\v\f\x1c\x1d\x1e\x85\u2028\u2029")


def split_segments(text, _scanned=0):
    """Split ``text[_scanned:]`` on the str.splitlines set — single
    pass, LINEAR in the NEW bytes only (repair2 R1).

    ``_scanned`` is the prefix length already known separator-free
    (only a trailing lone ``\\r`` can pair into ``\\r\\n`` across a
    chunk boundary, so at most one character of history matters);
    callers must keep that dangling trailing ``\\r`` OUT of the scanned
    prefix (see scan_new) or it stops acting as a separator.
    Returns ``(segments, tail_wait)``: complete lines plus the
    remainder that cannot yet be emitted.
    """
    segments = []
    rest = text
    start = _scanned
    while True:
        cut = None
        for index in range(start, len(rest)):
            if rest[index] in LINE_SEPARATORS:
                cut = index
                break
        if cut is None:
            break
        if rest[cut] == "\r":
            if cut + 1 == len(rest):
                break  # may become \r\n with the next chunk
            if rest[cut + 1] == "\n":
                segments.append(rest[:cut])
                rest = rest[cut + 2:]
                start = 0
                continue
        segments.append(rest[:cut])
        rest = rest[cut + 1:]
        start = 0
    return segments, rest


def scan_new(buffer, scanned):
    """Split ``buffer`` continuing from ``scanned`` (see
    split_segments); returns (segments, rest, new_scanned)."""
    segments, rest = split_segments(buffer, scanned)
    if segments:
        return segments, rest, 0
    # repair3 R3/N6: with no segment emitted, a DANGLING trailing \r
    # (the "may become \r\n" break) must stay OUT of the scanned
    # prefix — it still acts as a separator (or pairs into \r\n) once
    # the next chunk arrives. Folding it into the scanned prefix made
    # it inert and glued records across the chunk boundary.
    if rest.endswith("\r"):
        return segments, rest, len(rest) - 1
    return segments, rest, len(rest)


def split_segments_final(text, _scanned=0):
    """EOF variant of split_segments (WP-A item 4): a DANGLING trailing
    ``\\r`` has no next chunk to pair with, so it IS a separator here —
    exactly ``str.splitlines``. Returns ``(segments, tail)`` where the
    tail is the text after the last separator (``""`` when the text
    ends with a separator)."""
    segments = []
    rest = text
    start = _scanned
    while True:
        cut = None
        for index in range(start, len(rest)):
            if rest[index] in LINE_SEPARATORS:
                cut = index
                break
        if cut is None:
            break
        if rest[cut] == "\r":
            if cut + 1 == len(rest):
                # EOF: no next chunk — the lone \r splits now
                segments.append(rest[:cut])
                return segments, ""
            if rest[cut + 1] == "\n":
                segments.append(rest[:cut])
                rest = rest[cut + 2:]
                start = 0
                continue
        segments.append(rest[:cut])
        rest = rest[cut + 1:]
        start = 0
    return segments, rest


def eof_segments(pending, scanned):
    """The exact ``str.splitlines`` EOF emission for a stream whose
    un-emitted remainder is ``pending`` (scanned prefix ``scanned``):
    emit every separator-terminated segment, and the unterminated tail
    only when non-empty. A dangling trailing ``\\r`` terminates one
    (possibly empty) segment — the R4/N3 one-lost-empty exemption is
    closed (WP-A item 4)."""
    if pending.endswith("\r"):
        segments, _tail = split_segments_final(pending, scanned)
        return segments  # tail is "" after the trailing separator
    segments, tail = split_segments(pending, scanned)
    if tail:
        return segments + [tail]
    return segments
