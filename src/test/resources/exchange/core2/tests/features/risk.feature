Feature: An exchange should not allow to users to submit orders which break their limits

  @BasicRiskCheck
  Scenario: basic scenario

    Given New client Alice has a balance:
      | XBT | 2000000 |
    And New client Bob has a balance:
      | ETH | 699999 |

    When A client Alice could not place an BID order 101 at 30000@7 (type: GTC, symbol: ETH_XBT, reservePrice: 30000) due to RISK_NSF
    And A balance of a client Alice:
      | XBT | 2000000 |
    And A client Alice orders:
      | id | price | size | filled | reservePrice | side |

    Given 100000 XBT is added to the balance of a client Alice
    When A client Alice places an BID order 101 at 30000@7 (type: GTC, symbol: ETH_XBT, reservePrice: 30000)
    Then An ETH_XBT order book is:
      | bid | price | ask |
      | 7   | 30000 |     |
    And A balance of a client Alice:
      | XBT | 0 |
    And A client Alice orders:
      | id  | price | size | filled | reservePrice | side |
      | 101 | 30000 | 7    | 0      | 30000        | BID  |

    When A client Bob could not place an ASK order 102 at 30000@7 (type: IOC, symbol: ETH_XBT, reservePrice: 30000) due to RISK_NSF
    Then A balance of a client Bob:
      | ETH | 699999 |
    And A client Bob does not have active orders

    Given 1 ETH is added to the balance of a client Bob
    When A client Bob places an ASK order 102 at 30000@7 (type: IOC, symbol: ETH_XBT, reservePrice: 30000)
    Then The order 101 is fully matched. LastPx: 30000, LastQty: 7

    And A balance of a client Alice:
      | ETH | 700000 |
    And A balance of a client Bob:
      | XBT | 2100000 |

    And A client Alice does not have active orders
    And A client Bob does not have active orders

  @MoveOrdersUpAndDown
  Scenario: move orders UP and DOWN

    Given New client Alice has a balance:
      | ETH | 100000000 |

    When A client Alice could not place an ASK order 202 at 30000@1001 (type: GTC, symbol: ETH_XBT, reservePrice: 30000) due to RISK_NSF
    Then A balance of a client Alice:
      | ETH | 100000000 |
    And A client Alice does not have active orders

    When A client Alice places an ASK order 202 at 30000@1000 (type: GTC, symbol: ETH_XBT, reservePrice: 30000)
    Then A balance of a client Alice:
      | ETH | 0 |
    And A client Alice orders:
      | id  | price | size | filled | reservePrice | side |
      | 202 | 30000 | 1000 | 0      | 30000        | ASK  |

    When A client Alice moves a price to 40000 of the order 202
    Then A balance of a client Alice:
      | ETH | 0 |
    And A client Alice orders:
      | id  | price | size | filled | reservePrice | side |
      | 202 | 40000 | 1000 | 0      | 30000        | ASK  |

    When A client Alice moves a price to 20000 of the order 202
    Then A balance of a client Alice:
      | ETH | 0 |
    And A client Alice orders:
      | id  | price | size | filled | reservePrice | side |
      | 202 | 20000 | 1000 | 0      | 30000        | ASK  |


    Given New client Bob has a balance:
      | XBT | 94000000 |

    When A client Bob could not place an BID order 203 at 18000@500 (type: GTC, symbol: ETH_XBT, reservePrice: 19000) due to RISK_NSF
    Then A balance of a client Bob:
      | XBT | 94000000 |
    And A client Bob does not have active orders

    When A client Bob places an BID order 203 at 18000@500 (type: GTC, symbol: ETH_XBT, reservePrice: 18500)
    Then No trade events
    And An ETH_XBT order book is:
      |     | 20000 | 1000 |
      | 500 | 18000 |      |
    And A balance of a client Bob:
      | XBT | 1500000 |
    And A client Bob orders:
      | id  | price | size | filled | reservePrice | side |
      | 203 | 18000 | 500  | 0      | 18500        | BID  |

    When A client Bob could not move a price to 18501 of the order 203 due to MATCHING_MOVE_FAILED_PRICE_OVER_RISK_LIMIT
    Then A balance of a client Bob:
      | XBT | 1500000 |
    And A client Bob orders:
      | id  | price | size | filled | reservePrice | side |
      | 203 | 18000 | 500  | 0      | 18500        | BID  |
    And An ETH_XBT order book is:
      |     | 20000 | 1000 |
      | 500 | 18000 |      |

    When A client Bob moves a price to 18500 of the order 203
    Then A balance of a client Bob:
      | XBT | 1500000 |
    And An ETH_XBT order book is:
      |     | 20000 | 1000 |
      | 500 | 18500 |      |

    When A client Bob moves a price to 17500 of the order 203
    Then A balance of a client Bob:
      | XBT | 1500000 |
    And An ETH_XBT order book is:
      |     | 20000 | 1000 |
      | 500 | 17500 |      |

    When A client Alice moves a price to 16900 of the order 202
    Then The order 203 is fully matched. LastPx: 17500, LastQty: 500, bidderHoldPrice: 18500
    And A balance of a client Alice:
      | ETH | 0        |
      | XBT | 87500000 |
    And A client Alice orders:
      | id  | price | size | filled | reservePrice | side |
      | 202 | 16900 | 1000 | 500    | 30000        | ASK  |
    And An ETH_XBT order book is:
      |  | 16900 | 500 |
    Then A balance of a client Bob:
      | XBT | 6500000  |
      | ETH | 50000000 |
    And A client Bob does not have active orders

    When A client Alice cancels the remaining size 500 of the order 202
    Then A balance of a client Alice:
      | ETH | 50000000 |
      | XBT | 87500000 |
    And A client Alice does not have active orders
