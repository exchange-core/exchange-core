Feature: An exchange can add users, manage users and their assets

  Background:
    Given Users and their balances:
      | user  | asset | balance   |
      | Alice | ETH   | 100000000 |

  @UserAndBalance
  Scenario: add user and manage their balances
    Given Users and their balances:
      | user    | asset | balance   |
      | Bob     | XBT   | 100000000 |
      | Charlie | ETH   | 100000000 |
    Then A balance of a client Bob:
      | XBT | 100000000 |
    And A balance of a client Charlie:
      | ETH | 100000000 |

  @SuspendUser
  Scenario: suspend user with balances
    When Suspend Alice who has balances will return "USER_MGMT_USER_NOT_SUSPENDABLE_NON_EMPTY_ACCOUNTS"
    Then Status of Alice is "ACTIVE"

    Given -100000000 ETH is added to the balance of a client Alice

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