/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.kafka;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.Type;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class KafkaInternalFieldDescription
{
    /**
     * Describes an internal (managed by the connector) field which is added to each table row. The definition itself makes the row
     * show up in the tables (the columns are hidden by default, so they must be explicitly selected) but unless the field is hooked in using the
     * forBooleanValue/forLongValue/forBytesValue methods and the resulting FieldValueProvider is then passed into the appropriate row decoder, the fields
     * will be null. Most values are assigned in the {@link io.prestosql.plugin.kafka.KafkaRecordSet}.
     */
    public enum InternalField
    {
        /**
         * <tt>_partition_id</tt> - Kafka partition id.
         */
        PARTITION_ID_FIELD("_partition_id", "Partition Id"),

        /**
         * <tt>_partition_offset</tt> - The current offset of the message in the partition.
         */
        PARTITION_OFFSET_FIELD("_partition_offset", "Offset for the message within the partition"),

        /**
         * <tt>_message_corrupt</tt> - True if the row converter could not read the a message. May be null if the row converter does not set a value (e.g. the dummy row converter does not).
         */
        MESSAGE_CORRUPT_FIELD("_message_corrupt", "Message data is corrupt"),

        /**
         * <tt>_message</tt> - Represents the full topic as a text column. Format is UTF-8 which may be wrong for some topics. TODO: make charset configurable.
         */
        MESSAGE_FIELD("_message", "Message text"),

        /**
         * <tt>_message_length</tt> - length in bytes of the message.
         */
        MESSAGE_LENGTH_FIELD("_message_length", "Total number of message bytes"),

        /**
         * <tt>_key_corrupt</tt> - True if the row converter could not read the a key. May be null if the row converter does not set a value (e.g. the dummy row converter does not).
         */
        KEY_CORRUPT_FIELD("_key_corrupt", "Key data is corrupt"),

        /**
         * <tt>_key</tt> - Represents the key as a text column. Format is UTF-8 which may be wrong for topics. TODO: make charset configurable.
         */
        KEY_FIELD("_key", "Key text"),

        /**
         * <tt>_key_length</tt> - length in bytes of the key.
         */
        KEY_LENGTH_FIELD("_key_length", "Total number of key bytes");

        private static final Map<String, InternalField> BY_COLUMN_NAME =
                stream(InternalField.values())
                        .collect(toImmutableMap(InternalField::getColumnName, identity()));

        public static InternalField forColumnName(String columnName)
        {
            InternalField description = BY_COLUMN_NAME.get(columnName);
            checkArgument(description != null, "Unknown internal column name %s", columnName);
            return description;
        }

        private final String columnName;
        private final String comment;

        InternalField(String columnName, String comment)
        {
            checkArgument(!isNullOrEmpty(columnName), "name is null or is empty");
            this.columnName = columnName;
            this.comment = requireNonNull(comment, "comment is null");
        }

        public String getColumnName()
        {
            return columnName;
        }

        public String getComment()
        {
            return comment;
        }
    }

    public static class InternalFieldDescription
    {
        private final InternalField internalField;
        private final Type type;

        InternalFieldDescription(InternalField internalField, Type type)
        {
            this.internalField = requireNonNull(internalField, "internalField is null");
            this.type = requireNonNull(type, "type is null");
        }

        public String getColumnName()
        {
            return internalField.getColumnName();
        }

        private Type getType()
        {
            return type;
        }

        KafkaColumnHandle getColumnHandle(int index, boolean hidden)
        {
            return new KafkaColumnHandle(
                    getColumnName(),
                    getType(),
                    null,
                    null,
                    null,
                    false,
                    hidden,
                    true);
        }

        ColumnMetadata getColumnMetadata(boolean hidden)
        {
            return ColumnMetadata.builder()
                    .setName(internalField.getColumnName())
                    .setType(type)
                    .setComment(Optional.ofNullable(internalField.getComment()))
                    .setHidden(hidden)
                    .build();
        }
    }

    private final Map<InternalField, InternalFieldDescription> internalFields;

    @Inject
    public KafkaInternalFieldDescription()
    {
        internalFields = new ImmutableMap.Builder<InternalField, InternalFieldDescription>()
                .put(InternalField.PARTITION_ID_FIELD, new InternalFieldDescription(InternalField.PARTITION_ID_FIELD, BigintType.BIGINT))
                .put(InternalField.PARTITION_OFFSET_FIELD, new InternalFieldDescription(InternalField.PARTITION_OFFSET_FIELD, BigintType.BIGINT))
                .put(InternalField.MESSAGE_CORRUPT_FIELD, new InternalFieldDescription(InternalField.MESSAGE_CORRUPT_FIELD, BooleanType.BOOLEAN))
                .put(InternalField.MESSAGE_FIELD, new InternalFieldDescription(InternalField.MESSAGE_FIELD, createUnboundedVarcharType()))
                .put(InternalField.MESSAGE_LENGTH_FIELD, new InternalFieldDescription(InternalField.MESSAGE_LENGTH_FIELD, BigintType.BIGINT))
                .put(InternalField.KEY_CORRUPT_FIELD, new InternalFieldDescription(InternalField.KEY_CORRUPT_FIELD, BooleanType.BOOLEAN))
                .put(InternalField.KEY_FIELD, new InternalFieldDescription(InternalField.KEY_FIELD, createUnboundedVarcharType()))
                .put(InternalField.KEY_LENGTH_FIELD, new InternalFieldDescription(InternalField.KEY_LENGTH_FIELD, BigintType.BIGINT))
                .build();
    }

    /**
     * @return Map of {@link InternalFieldDescription} for each internal field.
     */
    public Map<InternalField, InternalFieldDescription> getInternalFields()
    {
        return internalFields;
    }
}
