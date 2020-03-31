Feature: An exchange should not allow to users to submit orders which break their limits

  Scenario: basic scenario

    Given New client A has a balance:
       | XBT | 2000000 |
    And New client B has a balance:
       | ETH | 699999 |

    When A client A could not place an BID order 101 at 30000@7 (type: GTC, symbol: ETH_XBT, reservePrice: 30000) due to RISK_NSF
     And A balance of a client A:
       | XBT | 2000000 |
     And A client A orders:
       | id | price | size | filled | reservePrice | side |

    Given 100000 XBT is added to the balance of a client A
    When A client A places an BID order 101 at 30000@7 (type: GTC, symbol: ETH_XBT, reservePrice: 30000)
    Then An ETH_XBT order book is:
      |  bid | price  | ask  |
      | 7    | 30000  |      |
    And A balance of a client A:
      | XBT | 0 |
    And A client A orders:
      | id  | price | size | filled | reservePrice | side |
      | 101 | 30000 | 7    | 0      | 30000        | BID  |

    When A client B could not place an ASK order 102 at 30000@7 (type: IOC, symbol: ETH_XBT, reservePrice: 30000) due to RISK_NSF
    Then A balance of a client B:
      | ETH | 699999 |
    And A client B does not have active orders

    Given 1 ETH is added to the balance of a client B
    When A client B places an ASK order 102 at 30000@7 (type: IOC, symbol: ETH_XBT, reservePrice: 30000)
    Then The order 101 is fully matched. LastPx: 30000, LastQty: 7

    And A balance of a client A:
      | ETH | 700000  |
    And A balance of a client B:
      | XBT | 2100000 |

    And A client A does not have active orders
    And A client B does not have active orders


 Scenario: move orders UP and DOWN

   Given New client A has a balance:
     | ETH | 100000000 |

   When A client A could not place an ASK order 202 at 30000@1001 (type: GTC, symbol: ETH_XBT, reservePrice: 30000) due to RISK_NSF
   Then A balance of a client A:
     | ETH | 100000000 |
   And A client A does not have active orders

   When A client A places an ASK order 202 at 30000@1000 (type: GTC, symbol: ETH_XBT, reservePrice: 30000)
   Then A balance of a client A:
     | ETH | 0 |
   And A client A orders:
     | id  | price | size | filled | reservePrice | side |
     | 202 | 30000 | 1000 | 0      | 30000        | ASK  |

   When A client A moves a price to 40000 of the order 202
   Then A balance of a client A:
     | ETH | 0 |
   And A client A orders:
     | id  | price | size | filled | reservePrice | side |
     | 202 | 40000 | 1000 | 0      | 30000        | ASK  |

   When A client A moves a price to 20000 of the order 202
   Then A balance of a client A:
     | ETH | 0 |
   And A client A orders:
     | id  | price | size | filled | reservePrice | side |
     | 202 | 20000 | 1000 | 0      | 30000        | ASK  |


   Given New client B has a balance:
      | XBT | 94000000 |

   When A client B could not place an BID order 203 at 18000@500 (type: GTC, symbol: ETH_XBT, reservePrice: 19000) due to RISK_NSF
   Then A balance of a client B:
     | XBT | 94000000 |
    And A client B does not have active orders

   When A client B places an BID order 203 at 18000@500 (type: GTC, symbol: ETH_XBT, reservePrice: 18500)
   Then No trade events
   And An ETH_XBT order book is:
      |       | 20000 | 1000 |
      |  500  | 18000 |      |
   And A balance of a client B:
      | XBT | 1500000 |
   And A client B orders:
     | id  | price | size | filled | reservePrice | side |
     | 203 | 18000 | 500 | 0       | 18500        | BID  |

   When A client B could not move a price to 18501 of the order 203 due to MATCHING_MOVE_FAILED_PRICE_OVER_RISK_LIMIT
   Then A balance of a client B:
     | XBT | 1500000 |
   And A client B orders:
     | id  | price | size | filled | reservePrice | side |
     | 203 | 18000 | 500 | 0       | 18500        | BID  |

   When A client B moves a price to 18500 of the order 203
   Then A balance of a client B:
     | XBT | 1500000 |

   When A client B moves a price to 17500 of the order 203
   Then A balance of a client B:
     | XBT | 1500000 |
   And An ETH_XBT order book is:
     |       | 20000 | 1000 |
     |  500  | 17500 |      |

   When A client A moves a price to 16900 of the order 202
   Then The order 203 is fully matched. LastPx: 17500, LastQty: 500, bidderHoldPrice: 18500
   And A balance of a client A:
     | ETH | 0        |
     | XBT | 87500000 |
   And A client A orders:
     | id  | price | size | filled  | reservePrice | side |
     | 202 | 16900 | 1000 | 500     | 30000        | ASK  |
   And An ETH_XBT order book is:
     |     | 16900 | 500  |
   Then A balance of a client B:
     | XBT |  6500000 |
     | ETH | 50000000 |
   And A client B does not have active orders

   When A client A cancels the remaining size 500 of the order 202
   Then A balance of a client A:
     | ETH | 50000000 |
     | XBT | 87500000 |
   And A client A does not have active orders

