"""Relative time expressions resolved to calendar dates.

"I went yesterday", written on Sat 2023-05-20, refers to Fri 2023-05-19.
`resolve` finds such expressions in a turn and the date each one refers to,
counted from the turn's own date; `annotate` inserts that date after the
expression: "I went yesterday [= Fri 2023-05-19]".

Resolved: yesterday / today / tomorrow (and "the day before yesterday",
"tonight", "this morning", ...), "N days|weeks|months|years ago" and
"... from now" (N a digit count or one..twelve / a / a few / a couple of),
"last|this past|next <weekday>", "last|this|next week|weekend|month|year".
Vague counts ("a few", "a couple of") and month/year shifts are marked
approximate ("≈" instead of "="). Left alone, because they are durations or
ambiguous: "in two weeks", "two days later", a bare weekday ("on Saturday"),
"the last week of June", "the next year", "the last night of the trip", a
number that is part of a larger one ("1.5 years ago", "1,000 years ago").

Dates are calendar dates of the turn's UTC timestamp; weekday and month
names are English whatever the process locale.
"""
from __future__ import annotations

import calendar
import datetime as _dt
import re

WEEKDAYS = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
WEEKDAY_ABBR = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
_MONTHS = ("January", "February", "March", "April", "May", "June", "July", "August",
           "September", "October", "November", "December")
_NUM = {"a": 1, "an": 1, "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7,
        "eight": 8, "nine": 9, "ten": 10, "eleven": 11, "twelve": 12, "a couple of": 2, "couple of": 2,
        "a couple": 2, "a few": 3, "few": 3}
_VAGUE = ("a couple of", "couple of", "a couple", "a few", "few")
# alternation order matters: the longer phrase first ("a couple of" before "a")
_NUMRE = (r"(\d{1,3}|a couple of|couple of|a couple|a few|few|an|a|one|two|three|four|five|six|seven|eight"
          r"|nine|ten|eleven|twelve)")
_WDRE = "(" + "|".join(WEEKDAYS) + ")"
# a count must not be the tail of a larger number: "1.5 years ago", "1,000 years ago", "2 1/2 years ago"
_NOT_PART = r"(?<!\d[.,/])"
# "the last week of June", "the next year": a span inside a phrase, not the one before/after the turn
_NOT_THE = r"(?<!the )"
_NOT_OF = r"(?!\s+of\b)"  # "last week of June" (no "the") is the same phrase
# "yesterday's meeting": the possessive belongs to the expression (the date goes after it)
_POSS = r"(?:['’]s\b)?"


def fmt_day(d: _dt.date) -> str:
    """'Sat 2023-05-20'."""
    return f"{WEEKDAY_ABBR[d.weekday()]} {d.year:04d}-{d.month:02d}-{d.day:02d}"


def utc_date(ms: int) -> _dt.date:
    return _dt.datetime.fromtimestamp(ms / 1000, _dt.timezone.utc).date()


def add_months(d: _dt.date, n: int) -> _dt.date:
    """d shifted by n calendar months, the day clamped to the target month's length."""
    m = d.month - 1 + n
    y, m = d.year + m // 12, m % 12 + 1
    return _dt.date(y, m, min(d.day, calendar.monthrange(y, m)[1]))


def _shift(d: _dt.date, n: int, unit: str) -> _dt.date:
    if unit == "day":
        return d + _dt.timedelta(days=n)
    if unit == "week":
        return d + _dt.timedelta(weeks=n)
    if unit == "month":
        return add_months(d, n)
    return add_months(d, 12 * n)


def _count(m: re.Match) -> tuple[int, str, bool]:
    raw, unit = m.group(1).lower(), m.group(2).lower()
    n = int(raw) if raw.isdigit() else _NUM[raw]
    return n, unit, raw in _VAGUE or unit in ("month", "year")


def _ago(m: re.Match, d: _dt.date) -> str:
    n, unit, approx = _count(m)
    return ("≈ " if approx else "= ") + fmt_day(_shift(d, -n, unit))


def _from_now(m: re.Match, d: _dt.date) -> str:
    n, unit, approx = _count(m)
    return ("≈ " if approx else "= ") + fmt_day(_shift(d, n, unit))


def _prev_weekday(m: re.Match, d: _dt.date) -> str:
    w = WEEKDAYS.index(m.group(2).lower())
    back = (d.weekday() - w) % 7 or 7  # strictly before the turn's date
    return "= " + fmt_day(d - _dt.timedelta(days=back))


def _next_weekday(m: re.Match, d: _dt.date) -> str:
    w = WEEKDAYS.index(m.group(1).lower())
    fwd = (w - d.weekday()) % 7 or 7  # strictly after the turn's date
    return "= " + fmt_day(d + _dt.timedelta(days=fwd))


def _span(m: re.Match, d: _dt.date) -> str:
    """last/this/next week (Mon-Sun), weekend (Sat-Sun of that week), month, year."""
    which, unit = m.group(1).lower(), m.group(2).lower()
    k = {"last": -1, "this": 0, "next": 1}[which]
    if unit in ("week", "weekend"):
        mon = d - _dt.timedelta(days=d.weekday()) + _dt.timedelta(weeks=k)
        first = mon + _dt.timedelta(days=5) if unit == "weekend" else mon
        last = mon + _dt.timedelta(days=6)
        return f"= {fmt_day(first)} to {fmt_day(last)}"
    if unit == "month":
        t = add_months(d.replace(day=1), k)
        return f"= {_MONTHS[t.month - 1]} {t.year:04d}"
    return f"= {d.year + k:04d}"


def _fixed(days: int):
    return lambda m, d: "= " + fmt_day(d + _dt.timedelta(days=days))


# (pattern, resolver(match, turn date) -> annotation); where two overlap at one
# position the longer wins ("the day before yesterday" over "yesterday")
_RULES = [
    (re.compile(r"\bthe day before yesterday\b" + _POSS, re.I), _fixed(-2)),
    (re.compile(r"\bthe day after tomorrow\b" + _POSS, re.I), _fixed(2)),
    (re.compile(r"\byesterday\b" + _POSS, re.I), _fixed(-1)),
    (re.compile(_NOT_THE + r"\blast night\b" + _POSS + _NOT_OF, re.I), _fixed(-1)),
    (re.compile(r"\btomorrow\b" + _POSS, re.I), _fixed(1)),
    (re.compile(r"\b(?:earlier today|today|tonight|this morning|this afternoon|this evening)\b" + _POSS, re.I),
     _fixed(0)),
    (re.compile(_NOT_PART + r"\b" + _NUMRE + r"\s+(day|week|month|year)s?\s+ago\b", re.I), _ago),
    # "N days from now" only: "in N days" / "N days later" are as often
    # durations, or relative to some other event
    (re.compile(_NOT_PART + r"\b" + _NUMRE + r"\s+(day|week|month|year)s?\s+from now\b", re.I), _from_now),
    (re.compile(_NOT_THE + r"\b(last|this past)\s+" + _WDRE + r"\b" + _POSS + _NOT_OF, re.I), _prev_weekday),
    (re.compile(_NOT_THE + r"\bnext\s+" + _WDRE + r"\b" + _POSS + _NOT_OF, re.I), _next_weekday),
    (re.compile(_NOT_THE + r"\b(last|this|next)\s+(weekend|week|month|year)\b" + _POSS + _NOT_OF, re.I), _span),
]


def resolve(text: str, day: _dt.date) -> list[tuple[int, int, str]]:
    """Non-overlapping (start, end, annotation) for the relative time
    expressions in `text`, leftmost first (on a tie, the longer match);
    `day` is the date the text was written on."""
    found = []
    for pat, fn in _RULES:
        for m in pat.finditer(text):
            try:
                found.append((m.start(), m.end(), fn(m, day)))
            except (ValueError, OverflowError):  # a date out of the calendar's range: leave it
                continue
    found.sort(key=lambda x: (x[0], -(x[1] - x[0])))
    out, last = [], -1
    for s, e, a in found:
        if s >= last:
            out.append((s, e, a))
            last = e
    return out


def annotate(text: str, day: _dt.date) -> str:
    """`text` with " [= Sat 2023-05-06]" after each resolvable expression."""
    out, pos = [], 0
    for s, e, a in resolve(text, day):
        out.append(text[pos:e])
        out.append(f" [{a}]")
        pos = e
    out.append(text[pos:])
    return "".join(out)
