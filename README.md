run on airlfow on vm

challenges:
- learn a bit about vm 
- code changes -> update code in vm
- secrests, variables and connections

automated pipeline














-------------------------

Attribute	Description
_id	Unique identifier for the entire test.
isPb	Indicates if the session resulted in a personal best (true/false).
wpm	Total number of characters in the correctly typed words (including spaces), divided by 5 and normalized to 60 seconds.
acc	Percentage of correctly pressed keys.
rawWpm	Calculated just like wpm, but also includes incorrect words.
consistency	Based on the variance of your raw WPM. Closer to 100% is better. Calculated using the coefficient of variation of raw WPM and mapped onto a scale from 0 to 100.
charStats	Number of correct, incorrect, extra, and missed characters (separated by semicolon).
mode	Typing mode used (time, words, quote, zen, and custom).
mode2	Sub-mode or additional typing configuration.
quoteLength	Length of the quote or text used for the session.
restartCount	Number of times the test was restarted before successful completion.
testDuration	Total duration of the typing test.
afkDuration	Time spent idle (away from the keyboard) during the test.
incompleteTestSeconds	Time spent in incomplete tests before finishing the test.
punctuation	Indicates if punctuation was included in the typing session (true/false).
numbers	Indicates if numbers were included in the typing session (true/false).
language	Language of the text used in the session (English by default).
funbox	Indicates if funbox mode was used (e.g., custom word lists).
difficulty	Difficulty level of the typing session (normal by default).
lazyMode	Indicates if lazy mode (relaxed typing rules) was enabled (true/false).
blindMode	Indicates if blind mode (hidden typed text) was enabled (true/false).
bailedOut	Indicates if the user exited the test prematurely (true/false).
tags	Tags or labels associated with the typing session.
timestamp	Date and time when the session occurred.


 incompleteTestSeconds
Meaning: Time spent in unfinished attempts — tests you started but didn’t finish, before the one you actually completed.

Triggers: You start typing, mess up or give up, exit the test, and start again.

Use case: Captures abandoned or reset tests before a successful one.

afkDuration
You start typing a 60s test, type for 20s, then go make coffee for 30s, return and finish.