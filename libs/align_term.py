__author__ = 'Russell'

import re
import unicodedata


def align_lemma(term, to_eng=True, remove_space=False, lower=False):
    # There are space situations:
    #   eng space eng, eng punc, eng chi, eng space num
    #   chi eng, chi punc, chi chi, chi num
    #   punc space eng, punc punc, punc chi, punc space num,
    #   num space eng, num punc, num chi, num space num
    # So, not chi space eng|num
    #   chi *
    #   not chi not eng|num

    # Defining regular expression pattern
    # Whether a char is Chinese
    pat_chi_str = r'[\u4e00-\u9fa5]'
    pat_chi = re.compile(pat_chi_str)
    # Normalizing space
    pat_space = re.compile(r'\s+')
    # Striping unnecessary spaces
    pat_strip = re.compile(
        r'(?<={0})\s+|(?<!{0})\s+(?![a-zA-Z0-9(À-𝚯])'.format(pat_chi_str)
    )
    # No spaces after left bracket and before right bracket
    pat_bracket = re.compile(r'(?<=[(\[{<"])\s+|\s+(?=[)\]}>"])')
    # No spaces between colon and num
    pat_colon = re.compile(r'(?<=:)\s+(?<=\d)')
    # No spaces before or after dash
    pat_dash = re.compile(r'\s*(-+)\s*')
    # One space before or after &
    pat_and_space = re.compile(
        r'(?<![A-Z0-9])\s*&\s*|\s*&\s*(?![0-9]|[A-Z][^a-z])'
    )
    # No spaces before or after &
    pat_and = re.compile(r'(?<=[A-Z0-9])\s*&\s*(?=[0-9]|[A-Z]([^a-z]|$))')

    # Mapping Chinese punctuations to English
    if to_eng:
        table = {
            ord(f): ord(t) for f, t in zip(
                '，。！？【】（）％＃＠＆１２３４５６７８９０“”‘’',
                ',.!?[]()%#@&1234567890""\'\''
            )
        }

        # try:
        #     term = json.loads('"{}"'.format(term.replace('"', '\\"')))
        # except json.JSONDecodeError as exc:
        #     print('{} <- {}'.format(exc, term))
        # Dealing with \ufeff
        term = unicodedata.normalize('NFKC', term)
        term = term.translate(table)
    if remove_space:
        # Dealing with \\u
        term = term.replace('\ufeff', '')
        # Dealing with NULL \x00
        # \\u0000 -> json.loads() -> \x00
        term = term.replace('\x00', '')

        term = pat_strip.sub('', term)
        term = pat_bracket.sub('', term)
        term = pat_colon.sub('', term)
        term = pat_dash.sub(r'\1', term)
        term = pat_and_space.sub(' & ', term)
        term = pat_and.sub('&', term)
        term = pat_space.sub(' ', term).strip()
    if lower:
        term = term.lower()

    return term
