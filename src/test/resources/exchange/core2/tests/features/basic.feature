Feature: An exchange accepts bid\ask orders, manage and publish order book and match cross orders

  Scenario Outline: basic full cycle test

    When A client 1440001 places an ASK order 101 at 1600@7 (type: GTC, symbol: <symbol>)
     And A client 1440001 places an BID order 102 at 1550@4 (type: GTC, symbol: <symbol>, reservePrice: 1561)
    Then An <symbol> order book is:
       |  bid | price  | ask  |
       |      | 1600   | 7    |
       | 4    | 1550   |      |
    And No trade events

    When A client 1440002 places an BID order 201 at 1700@2 (type: IOC, symbol: <symbol>, reservePrice: 1800)
    Then The order 101 is partially matched. LastPx: 1600, LastQty: 2
    And An <symbol> order book is:
      |      | 1600   | 5    |
      | 4    | 1550   |      |

    When A client 1440002 places an BID order 202 at 1583@4 (type: GTC, symbol: <symbol>, reservePrice: 1583)
    Then An <symbol> order book is:
      |      | 1600   | 5    |
      | 4    | 1583   |      |
      | 4    | 1550   |      |
    And No trade events

    When A client 1440001 moves a price to 1580 of the order 101
    Then The order 202 is fully matched. LastPx: 1583, LastQty: 4
    And An <symbol> order book is:
      |      | 1580   | 1    |
      | 4    | 1550   |      |

    Examples:
    | symbol  |
    | EUR_USD |
    | ETH_XBT |

