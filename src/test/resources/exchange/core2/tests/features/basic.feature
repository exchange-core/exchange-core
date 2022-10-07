Feature: An exchange accepts bid\ask orders, manage and publish order book and match cross orders

  Background:
    Given Users and their balances:
      | user  | asset | balance   |
      | Alice | USD   | 1000000   |
      | Alice | XBT   | 100000000 |
      | Alice | ETH   | 100000000 |
      | Bob   | USD   | 2000000   |
      | Bob   | XBT   | 100000000 |
      | Bob   | ETH   | 100000000 |

  @BasicFullCycleTest
  Scenario Outline: basic full cycle test

    When A client Alice places an ASK order 101 at 1600@7 (type: GTC, symbol: <symbol>)
    And A client Alice places an BID order 102 at 1550@4 (type: GTC, symbol: <symbol>, reservePrice: 1561)
    Then An <symbol> order book is:
      | bid | price | ask |
      |     | 1600  | 7   |
      | 4   | 1550  |     |
    And No trade events
    And A client Alice orders:
      | id  | price | size | filled | reservePrice | side |
      | 101 | 1600  | 7    | 0      | 0            | ASK  |
      | 102 | 1550  | 4    | 0      | 1561         | BID  |

    When A client Bob places an BID order 201 at 1700@2 (type: IOC, symbol: <symbol>, reservePrice: 1800)
    Then The order 101 is partially matched. LastPx: 1600, LastQty: 2
    And An <symbol> order book is:
      |   | 1600 | 5 |
      | 4 | 1550 |   |

    When A client Bob places an BID order 202 at 1583@4 (type: GTC, symbol: <symbol>, reservePrice: 1583)
    Then An <symbol> order book is:
      |   | 1600 | 5 |
      | 4 | 1583 |   |
      | 4 | 1550 |   |
    And No trade events

    When A client Alice moves a price to 1580 of the order 101
    Then The order 202 is fully matched. LastPx: 1583, LastQty: 4
    And An <symbol> order book is:
      |   | 1580 | 1 |
      | 4 | 1550 |   |

    Examples:
      | symbol  |
      | EUR_USD |
      | ETH_XBT |

  @CancelBidOrder
  Scenario: cancel BID order
    Given Users and their balances:
      | user    | asset | balance  |
      | Charlie | XBT   | 94000000 |

    When A client Charlie places an BID order 203 at 18500@500 (type: GTC, symbol: ETH_XBT, reservePrice: 18500)

    Then A balance of a client Charlie:
      | ETH | 0       |
      | XBT | 1500000 |

    And A client Charlie orders:
      | id  | price | size | filled | reservePrice | side |
      | 203 | 18500 | 500  | 0      | 18500        | BID  |

    And An ETH_XBT order book is:
      | 500 | 18500 |  |

    When A client Charlie cancels the remaining size 500 of the order 203
    Then A client Charlie does not have active orders
    And A balance of a client Charlie:
      | ETH | 0        |
      | XBT | 94000000 |

  @SuspendUser
  Scenario: suspend user with balances
    When Suspend Alice who has balances will return "USER_MGMT_USER_NOT_SUSPENDABLE_NON_EMPTY_ACCOUNTS"
    Then Status of Alice is "ACTIVE"

    Given -1000000 USD is added to the balance of a client Alice
    And -100000000 XBT is added to the balance of a client Alice
    And -100000000 ETH is added to the balance of a client Alice

    When Suspend Alice who has no balances
    Then Query Alice will return "USER_NOT_FOUND"

  Scenario: suspend user with zero balances
    Given Users and their balances:
      | user    | asset | balance |
      | Charlie | XBT   | 0       |
    When Suspend Charlie who has no balances
    Then Query Charlie will return "USER_NOT_FOUND"

  Scenario: suspend user with open orders
    Given Users and their balances:
      | user    | asset | balance |
      | Charlie | XBT   | 18000   |
    When A client Charlie places an BID order 203 at 1800@1 (type: GTC, symbol: ETH_XBT, reservePrice: 1800)
    Then A balance of a client Charlie:
      | ETH | 0 |
      | XBT | 0 |
    And A client Charlie orders:
      | id  | price | size | filled | reservePrice | side |
      | 203 | 1800  | 1    | 0      | 1800         | BID  |

    When Suspend Charlie who has no balances
    Then Query Charlie will return "USER_NOT_FOUND"

    When A client Alice places an ASK order 102 at 1800@4 (type: GTC, symbol: ETH_XBT, reservePrice: 1800)
    Then A client Charlie does not have active orders
    And Query Charlie will return "OK"
