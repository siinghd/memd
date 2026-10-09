"""Relative time expressions resolved to calendar dates (memd.query.dates).

Session packing can annotate "I went yesterday" as "I went yesterday
[= Fri 2023-05-19]", counted from the turn's own date. A wrong annotation is
worse than none, so most of these tests are about what must NOT match.
"""
import datetime as dt
import locale
import time

import pytest

from memd.query import dates

S = dt.date(2023, 5, 20)  # a Saturday


def ann(text, d=S):
    return [(text[s:e], a) for s, e, a in dates.resolve(text, d)]


# ---------------------------------------------------------------- what resolves

def test_days():
    assert ann("I went yesterday") == [("yesterday", "= Fri 2023-05-19")]
    assert ann("the day before yesterday") == [("the day before yesterday", "= Thu 2023-05-18")]
    assert ann("the day after tomorrow") == [("the day after tomorrow", "= Mon 2023-05-22")]
    assert ann("tomorrow") == [("tomorrow", "= Sun 2023-05-21")]
    assert ann("Today I fixed it") == [("Today", "= Sat 2023-05-20")]
    for phrase in ("earlier today", "tonight", "this morning", "this afternoon", "this evening"):
        assert ann(f"I saw it {phrase}.") == [(phrase, "= Sat 2023-05-20")], phrase
    assert ann("last night was loud") == [("last night", "= Fri 2023-05-19")]


def test_counts_ago_and_from_now():
    assert ann("I started two weeks ago and 3 days ago") == [("two weeks ago", "= Sat 2023-05-06"),
                                                            ("3 days ago", "= Wed 2023-05-17")]
    assert ann("a week ago") == [("a week ago", "= Sat 2023-05-13")]
    assert ann("an hour or one day ago") == [("one day ago", "= Fri 2023-05-19")]
    assert ann("12 days ago") == [("12 days ago", "= Mon 2023-05-08")]
    assert ann("twelve weeks ago") == [("twelve weeks ago", "= Sat 2023-02-25")]
    assert ann("Two weeks ago") == [("Two weeks ago", "= Sat 2023-05-06")]  # any case at a sentence's start
    assert ann("TWO weeks AGO") == [("TWO weeks AGO", "= Sat 2023-05-06")]
    assert ann("2 week ago") == [("2 week ago", "= Sat 2023-05-06")]  # singular unit
    assert ann("5 days from now") == [("5 days from now", "= Thu 2023-05-25")]
    assert ann("two weeks from now") == [("two weeks from now", "= Sat 2023-06-03")]


def test_vague_counts_and_months_years_are_approximate():
    assert ann("a couple of weeks ago") == [("a couple of weeks ago", "≈ Sat 2023-05-06")]
    assert ann("a couple days ago") == [("a couple days ago", "≈ Thu 2023-05-18")]
    assert ann("a few days ago") == [("a few days ago", "≈ Wed 2023-05-17")]
    assert ann("few weeks from now") == [("few weeks from now", "≈ Sat 2023-06-10")]
    assert ann("2 months ago") == [("2 months ago", "≈ Mon 2023-03-20")]
    assert ann("a year ago") == [("a year ago", "≈ Fri 2022-05-20")]
    assert ann("a month from now") == [("a month from now", "≈ Tue 2023-06-20")]


def test_month_shifts_clamp_to_month_end():
    assert ann("3 months ago", dt.date(2023, 5, 31)) == [("3 months ago", "≈ Tue 2023-02-28")]
    assert ann("a month ago", dt.date(2024, 3, 31)) == [("a month ago", "≈ Thu 2024-02-29")]  # leap year
    assert ann("a year ago", dt.date(2024, 2, 29)) == [("a year ago", "≈ Tue 2023-02-28")]
    assert ann("2 months from now", dt.date(2023, 12, 31)) == [("2 months from now", "≈ Thu 2024-02-29")]


def test_weekdays_strictly_before_or_after_the_turn():
    assert ann("last Saturday") == [("last Saturday", "= Sat 2023-05-13")]  # not the turn's own day
    assert ann("last Friday") == [("last Friday", "= Fri 2023-05-19")]
    assert ann("this past Monday") == [("this past Monday", "= Mon 2023-05-15")]
    assert ann("next Saturday") == [("next Saturday", "= Sat 2023-05-27")]
    assert ann("next Sunday") == [("next Sunday", "= Sun 2023-05-21")]
    assert ann("LAST TUESDAY") == [("LAST TUESDAY", "= Tue 2023-05-16")]


def test_spans_weeks_months_years():
    assert ann("last week") == [("last week", "= Mon 2023-05-08 to Sun 2023-05-14")]
    assert ann("this week") == [("this week", "= Mon 2023-05-15 to Sun 2023-05-21")]
    assert ann("next week") == [("next week", "= Mon 2023-05-22 to Sun 2023-05-28")]
    assert ann("last weekend", dt.date(2023, 5, 17)) == [("last weekend", "= Sat 2023-05-13 to Sun 2023-05-14")]
    assert ann("this weekend", dt.date(2023, 5, 17)) == [("this weekend", "= Sat 2023-05-20 to Sun 2023-05-21")]
    assert ann("last month") == [("last month", "= April 2023")]
    assert ann("next month") == [("next month", "= June 2023")]
    assert ann("last year") == [("last year", "= 2022")]
    assert ann("this year") == [("this year", "= 2023")]


def test_year_boundaries():
    jan2 = dt.date(2023, 1, 2)  # a Monday
    assert ann("last week", jan2) == [("last week", "= Mon 2022-12-26 to Sun 2023-01-01")]
    assert ann("last month", jan2) == [("last month", "= December 2022")]
    assert ann("yesterday", dt.date(2023, 1, 1)) == [("yesterday", "= Sat 2022-12-31")]
    assert ann("next month", dt.date(2023, 12, 15)) == [("next month", "= January 2024")]
    assert ann("tomorrow", dt.date(2023, 12, 31)) == [("tomorrow", "= Mon 2024-01-01")]


def test_possessive_keeps_the_annotation_after_the_word():
    assert ann("yesterday's meeting") == [("yesterday's", "= Fri 2023-05-19")]
    assert dates.annotate("Today's plan", S) == "Today's [= Sat 2023-05-20] plan"
    assert dates.annotate("last week’s notes", S) == "last week’s [= Mon 2023-05-08 to Sun 2023-05-14] notes"


def test_overlaps_resolve_to_the_longest_leftmost():
    assert ann("the day before yesterday") == [("the day before yesterday", "= Thu 2023-05-18")]
    assert ann("earlier today") == [("earlier today", "= Sat 2023-05-20")]
    assert ann("this past Monday") == [("this past Monday", "= Mon 2023-05-15")]


def test_annotate_inserts_after_each_expression():
    assert dates.annotate("I left yesterday.", S) == "I left yesterday [= Fri 2023-05-19]."
    assert (dates.annotate("Yesterday I booked it for next Friday; paid 2 days ago!", S)
            == "Yesterday [= Fri 2023-05-19] I booked it for next Friday [= Fri 2023-05-26]; "
               "paid 2 days ago [= Thu 2023-05-18]!")
    assert dates.annotate("nothing to see", S) == "nothing to see"
    assert dates.annotate("", S) == ""


# ---------------------------------------------------------------- what must not

@pytest.mark.parametrize("text", [
    "I finished it in two weeks",           # a duration
    "two days later we left",               # relative to another event
    "in 3 days",
    "on Saturday",                          # a bare weekday is ambiguous
    "Mondays are slow",
    "the last week of June",                # a span inside a phrase
    "last week of June",
    "the next year or so",
    "in the last month",
    "the last night of the trip",
    "the last Friday of the month",
    "1.5 years ago",                        # part of a larger number
    "1,000 years ago",
    "2 1/2 years ago",
    "weeks ago",                            # no count
    "an hour ago",                          # unit not handled
    "a while ago",
    "ages ago",
    "yesterdays",                           # not the word
    "lastweek",
    "nextweek is a typo",
    "twenty years ago",                     # count not handled: nothing rather than a guess
    "the past year",
    "since last",
    "My day was good, the weeks flew",
])
def test_no_false_positives(text):
    assert ann(text) == [], text


@pytest.mark.parametrize("text", [
    "I read USA Today every morning",          # capitalized in mid-sentence: a name
    "We watched The Tonight Show",
    "Last Week Tonight with John Oliver",      # every word capitalized: a title
    "Last Night a DJ Saved My Life",
    "This Week in Tech podcast",
    "The Day Before Yesterday",
    "Tomorrow Never Dies (film)",              # followed by a capitalized word
    "I watched Next Friday again",
    "Two Weeks Ago",
])
def test_titles_and_names_are_left_alone(text):
    assert ann(text) == [], text


@pytest.mark.parametrize("text", [
    "Yesterday by the Beatles is my favourite song",   # a work and who made it
    "Yesterday by the Beatles.",
    "ok. Yesterday by Paul McCartney, 1965",
    "Next Friday is a movie",                          # what it is: a work
    "Next Friday is a 2000 comedy film",
    "Tomorrow is a song by Brian Eno",
    "Yesterday was a Beatles single",
])
def test_a_title_at_a_sentence_start_is_left_alone(text):
    assert ann(text) == [], text


@pytest.mark.parametrize("text, expr", [
    ("Yesterday by the lake we swam", "Yesterday"),            # "by" a place, lowercase
    ("Yesterday by the Thames we had a picnic", "Yesterday"),  # a subject follows the name
    ("Yesterday by Noon I was done", "Yesterday"),
    ("Yesterday, by the way, I left", "Yesterday"),
    ("Tomorrow is a big day", "Tomorrow"),
    ("Tomorrow is a movie night", "Tomorrow"),
    ("Next Friday is a holiday", "Next Friday"),
    ("I left yesterday by train", "yesterday"),
])
def test_dates_near_those_words_still_resolve(text, expr):
    assert [e for e, _a in ann(text)] == [expr], text


def test_sentence_starts_still_resolve():
    assert ann("Yesterday I went") == [("Yesterday", "= Fri 2023-05-19")]
    assert ann("Last week was busy") == [("Last week", "= Mon 2023-05-08 to Sun 2023-05-14")]
    assert ann("ok. Tomorrow we fly") == [("Tomorrow", "= Sun 2023-05-21")]
    assert ann('She said: "Today is fine"') == [("Today", "= Sat 2023-05-20")]
    assert ann("Next Friday we fly") == [("Next Friday", "= Fri 2023-05-26")]  # a title or not: can't tell


def test_ranges_are_approximate_spans():
    assert ann("3-4 days ago") == [("3-4 days ago", "≈ Tue 2023-05-16 to Wed 2023-05-17")]
    assert ann("2 to 3 weeks ago") == [("2 to 3 weeks ago", "≈ Sat 2023-04-29 to Sat 2023-05-06")]
    assert ann("two or three days ago") == [("two or three days ago", "≈ Wed 2023-05-17 to Thu 2023-05-18")]
    assert ann("1–2 weeks from now") == [("1–2 weeks from now", "≈ Sat 2023-05-27 to Sat 2023-06-03")]
    assert ann("1.5-2 years ago") == []  # a part of a larger number, still


def test_suffixes_are_not_the_word():
    assert ann("today-ish") == [] and ann("tomorrowland") == [] and ann("last years") == []


def test_dates_out_of_the_calendar_are_left_alone():
    assert ann("tomorrow", dt.date(9999, 12, 31)) == []
    assert ann("yesterday", dt.date(1, 1, 1)) == []
    assert ann("3 years ago and yesterday", dt.date(2, 1, 1)) == [("yesterday", "= Mon 0001-12-31")]
    assert ann("next year", dt.date(9999, 6, 1)) == []
    assert ann("last year", dt.date(1, 6, 1)) == []
    assert ann("next month", dt.date(9999, 12, 1)) == []


def test_names_do_not_depend_on_the_locale():
    for loc in ("de_DE.UTF-8", "fr_FR.UTF-8"):
        try:
            old = locale.setlocale(locale.LC_TIME)
            locale.setlocale(locale.LC_TIME, loc)
        except locale.Error:
            continue
        try:
            assert ann("last month") == [("last month", "= April 2023")]
            assert dates.fmt_day(S) == "Sat 2023-05-20"
        finally:
            locale.setlocale(locale.LC_TIME, old)
    assert dates.fmt_day(dt.date(999, 1, 1)) == "Tue 0999-01-01"


def test_utc_date_ignores_the_process_timezone(monkeypatch):
    ms = int(dt.datetime(2023, 5, 20, 1, 30, tzinfo=dt.timezone.utc).timestamp() * 1000)
    monkeypatch.setenv("TZ", "America/Los_Angeles")
    time.tzset()
    try:
        assert dates.utc_date(ms) == dt.date(2023, 5, 20)  # it is still the 19th in Los Angeles
    finally:
        monkeypatch.undo()
        time.tzset()


def test_resolve_is_deterministic_and_sorted():
    text = "next week, yesterday, 2 days ago, last Monday, tomorrow"
    a = dates.resolve(text, S)
    assert a == dates.resolve(text, S)
    assert [s for s, _, _ in a] == sorted(s for s, _, _ in a)
    assert all(e1 <= s2 for (_, e1, _), (s2, _, _) in zip(a, a[1:]))
