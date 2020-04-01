#!/usr/bin/env python
# -*- coding: utf-8 -*-

# -----------------
# Реализуйте функцию best_hand, которая принимает на вход
# покерную "руку" (hand) из 7ми карт и возвращает лучшую
# (относительно значения, возвращаемого hand_rank)
# "руку" из 5ти карт. У каждой карты есть масть(suit) и
# ранг(rank)
# Масти: трефы(clubs, C), пики(spades, S), червы(hearts, H), бубны(diamonds, D)
# Ранги: 2, 3, 4, 5, 6, 7, 8, 9, 10 (ten, T), валет (jack, J), дама (queen, Q), король (king, K), туз (ace, A)
# Например: AS - туз пик (ace of spades), TH - дестяка черв (ten of hearts), 3C - тройка треф (three of clubs)

# Задание со *
# Реализуйте функцию best_wild_hand, которая принимает на вход
# покерную "руку" (hand) из 7ми карт и возвращает лучшую
# (относительно значения, возвращаемого hand_rank)
# "руку" из 5ти карт. Кроме прочего в данном варианте "рука"
# может включать джокера. Джокеры могут заменить карту любой
# масти и ранга того же цвета, в колоде два джокерва.
# Черный джокер '?B' может быть использован в качестве треф
# или пик любого ранга, красный джокер '?R' - в качестве черв и бубен
# любого ранга.

# Одна функция уже реализована, сигнатуры и описания других даны.
# Вам наверняка пригодится itertools.
# Можно свободно определять свои функции и т.п.
# -----------------
import itertools


def hand_rank(hand):
    """Возвращает значение определяющее ранг 'руки'"""
    ranks = card_ranks(hand)
    if straight(ranks) and flush(hand):
        return 8, max(ranks)
    elif kind(4, ranks):
        return 7, kind(4, ranks), kind(1, ranks)
    elif kind(3, ranks) and kind(2, ranks):
        return 6, kind(3, ranks), kind(2, ranks)
    elif flush(hand):
        return 5, ranks
    elif straight(ranks):
        return 4, max(ranks)
    elif kind(3, ranks):
        return 3, kind(3, ranks), ranks
    elif two_pair(ranks):
        return 2, two_pair(ranks), ranks
    elif kind(2, ranks):
        return 1, kind(2, ranks), ranks
    else:
        return 0, ranks


def card_ranks(hand):
    """Возвращает список рангов (его числовой эквивалент),
    отсортированный от большего к меньшему"""
    ranks = '23456789TJQKA'
    return sorted([ranks.index(rank[0]) for rank in hand], reverse=True)


def flush(hand):
    """Возвращает True, если все карты одной масти"""
    hand_suit = set([rank[1] for rank in hand])
    return len(hand_suit) == 1


def straight(ranks):
    """Возвращает True, если отсортированные ранги формируют последовательность 5ти,
    где у 5ти карт ранги идут по порядку (стрит)"""
    ranks_hand = sorted(set(ranks[ranks.count(15):]), reverse=True)
    result = max_result = 1
    j = ranks_hand[0]
    for i in ranks_hand[1:]:
        if j == i+1:
            result += 1
            if result > max_result:
                max_result = result
        else:
            result = 1
        j = i
    result = max(result, max_result) >= 5
    return result


def kind(n, ranks):
    """Возвращает первый ранг, который n раз встречается в данной руке.
    Возвращает None, если ничего не найдено"""
    ranks_kind = ranks[ranks.count(15):]
    for i in sorted(set(ranks_kind), reverse=True):
        if ranks_kind.count(i) == n:
            return i
    return None


def two_pair(ranks):
    """Если есть две пары, то возврщает два соответствующих ранга,
    иначе возвращает None"""
    ranks_two_pairs = ranks[ranks.count(15):]
    pairs = {i: ranks_two_pairs.count(i) for i in sorted(set(ranks_two_pairs), reverse=True)}
    result = []
    [result.append(r) if i >= 2 else None for r, i in pairs.items()]
    if len(result) > 2:
        return result[:2]
    return None


def best_hand(hand):
    """Из "руки" в 7 карт возвращает лучшую "руку" в 5 карт """
    combinations_hand_five = itertools.combinations(hand, 5)
    result = max(combinations_hand_five, key=hand_rank)
    return result


def best_wild_hand(hand):
    """best_hand но с джокерами"""
    joker = {'?B': 'CS', '?R': 'HD'}
    ranks = '23456789TJQKA'
    hand_without_joker = [i for i in hand if i not in joker]
    suits_joker = [joker[i] for i in hand if i in joker]
    comb_hands = [hand_without_joker]
    for suit_joker in suits_joker:
        combs_joker = ['%s%s' % (rank, suit) for rank, suit in itertools.product(ranks, suit_joker)]
        combs_joker = [comb_joker for comb_joker in combs_joker if comb_joker not in hand_without_joker]
        comb_hands = [h + [card] for h in comb_hands for card in combs_joker]
    result = max(set(best_hand(combine) for combine in comb_hands), key=hand_rank)
    return result


def test_best_hand():
    print("test_best_hand...")
    assert (sorted(best_hand("6C 7C 8C 9C TC 5C JS".split()))
            == ['6C', '7C', '8C', '9C', 'TC'])
    assert (sorted(best_hand("TD TC TH 7C 7D 8C 8S".split()))
            == ['8C', '8S', 'TC', 'TD', 'TH'])
    assert (sorted(best_hand("JD TC TH 7C 7D 7S 7H".split()))
            == ['7C', '7D', '7H', '7S', 'JD'])
    print('OK')


def test_best_wild_hand():
    print("test_best_wild_hand...")
    assert (sorted(best_wild_hand("6C 7C 8C 9C TC 5C ?B".split()))
            == ['7C', '8C', '9C', 'JC', 'TC'])
    assert (sorted(best_wild_hand("TD TC 5H 5C 7C ?R ?B".split()))
            == ['7C', 'TC', 'TD', 'TH', 'TS'])
    assert (sorted(best_wild_hand("JD TC TH 7C 7D 7S 7H".split()))
            == ['7C', '7D', '7H', '7S', 'JD'])

    assert (sorted(best_wild_hand("2D 5D 6D 7D 8D ?B ?R".split()))
            == ['5D', '6D', '7D', '8D', '9D'])

    print('OK')


if __name__ == '__main__':
    test_best_hand()
    test_best_wild_hand()
    pass
