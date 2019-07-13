package org.openpredict.exchange.beans.reports;


import lombok.AllArgsConstructor;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import org.openpredict.exchange.beans.ReportType;

import java.util.Arrays;
import java.util.stream.Stream;

@AllArgsConstructor
@Getter
public class StateHashReportResult implements ReportResult {

    // state hash
    private int stateHash;


    private StateHashReportResult(final BytesIn bytesIn) {
        this.stateHash = bytesIn.readInt();
    }

    @Override
    public void writeMarshallable(final BytesOut bytes) {
        bytes.writeInt(stateHash);
    }

    public static StateHashReportResult merge(final Stream<BytesIn> pieces) {

        return new StateHashReportResult(
                Arrays.hashCode(pieces
                        .map(StateHashReportResult::new)
                        .mapToInt(StateHashReportResult::getStateHash)
                        .sorted() // make deterministic
                        .toArray()));
    }

}
