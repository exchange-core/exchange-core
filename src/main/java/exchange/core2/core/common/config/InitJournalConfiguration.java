package exchange.core2.core.common.config;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class InitJournalConfiguration {

    public static InitJournalConfiguration IGNORE_JOURNAL = new InitJournalConfiguration(Long.MIN_VALUE);

    @Getter
    private final long timestampNs; // exclusive timestamp,
    // Long.MAX_VALUE to read full available journal (or until reading error)
    // 0 to ignore the journal

    public static InitJournalConfiguration ofTime(long timestampNs) {
        return new InitJournalConfiguration(timestampNs);
    }


}
