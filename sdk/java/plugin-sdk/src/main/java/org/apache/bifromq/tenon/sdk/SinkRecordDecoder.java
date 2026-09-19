/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bifromq.tenon.sdk;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import java.io.IOException;
import java.util.Objects;
import org.apache.bifromq.tenon.contracts.sink.EgressRecordOuterClass.EgressRecord;

/** Validates one reader-owned EgressRecord and decodes its generated Payload. */
final class SinkRecordDecoder {
  private SinkRecordDecoder() {}

  /** Decodes reader-owned bytes with the payload parser selected by this Queue's owner. */
  static <P extends MessageLite> P decode(ByteString recordBytes, Parser<P> payloadParser)
      throws SinkRecordDecodeException {
    Objects.requireNonNull(recordBytes, "recordBytes");
    Objects.requireNonNull(payloadParser, "payloadParser");

    var record =
        parse(
            recordBytes,
            EgressRecord.parser(),
            "sink.egress_record_invalid",
            "Invalid EgressRecord");
    return parse(
        record.getPayload(), payloadParser, "sink.payload_invalid", "Invalid SinkRecordPayload");
  }

  private static <P extends MessageLite> P parse(
      ByteString bytes, Parser<P> parser, String code, String message)
      throws SinkRecordDecodeException {
    CodedInputStream input = bytes.newCodedInput();
    input.enableAliasing(true);
    try {
      var decoded = parser.parseFrom(input);
      input.checkLastTagWas(0);
      if (!input.isAtEnd()) {
        throw new IOException("Protobuf input was not consumed completely");
      }
      return decoded;
    } catch (IOException error) {
      throw new SinkRecordDecodeException(code, message, error);
    }
  }
}
