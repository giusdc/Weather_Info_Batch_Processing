/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package utils;

import org.apache.avro.specific.SpecificData;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class cityAttributes extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -7249729537400346034L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"cityAttributes\",\"namespace\":\"utils\",\"fields\":[{\"name\":\"city\",\"type\":\"string\"},{\"name\":\"Latitude\",\"type\":\"string\"},{\"name\":\"Longitude\",\"type\":\"string\"}],\"version\":\"1\"}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<cityAttributes> ENCODER =
      new BinaryMessageEncoder<cityAttributes>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<cityAttributes> DECODER =
      new BinaryMessageDecoder<cityAttributes>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   */
  public static BinaryMessageDecoder<cityAttributes> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   */
  public static BinaryMessageDecoder<cityAttributes> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<cityAttributes>(MODEL$, SCHEMA$, resolver);
  }

  /** Serializes this cityAttributes to a ByteBuffer. */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /** Deserializes a cityAttributes from a ByteBuffer. */
  public static cityAttributes fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  @Deprecated public CharSequence city;
  @Deprecated public CharSequence Latitude;
  @Deprecated public CharSequence Longitude;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public cityAttributes() {}

  /**
   * All-args constructor.
   * @param city The new value for city
   * @param Latitude The new value for Latitude
   * @param Longitude The new value for Longitude
   */
  public cityAttributes(CharSequence city, CharSequence Latitude, CharSequence Longitude) {
    this.city = city;
    this.Latitude = Latitude;
    this.Longitude = Longitude;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public Object get(int field$) {
    switch (field$) {
    case 0: return city;
    case 1: return Latitude;
    case 2: return Longitude;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, Object value$) {
    switch (field$) {
    case 0: city = (CharSequence)value$; break;
    case 1: Latitude = (CharSequence)value$; break;
    case 2: Longitude = (CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'city' field.
   * @return The value of the 'city' field.
   */
  public CharSequence getCity() {
    return city;
  }

  /**
   * Sets the value of the 'city' field.
   * @param value the value to set.
   */
  public void setCity(CharSequence value) {
    this.city = value;
  }

  /**
   * Gets the value of the 'Latitude' field.
   * @return The value of the 'Latitude' field.
   */
  public CharSequence getLatitude() {
    return Latitude;
  }

  /**
   * Sets the value of the 'Latitude' field.
   * @param value the value to set.
   */
  public void setLatitude(CharSequence value) {
    this.Latitude = value;
  }

  /**
   * Gets the value of the 'Longitude' field.
   * @return The value of the 'Longitude' field.
   */
  public CharSequence getLongitude() {
    return Longitude;
  }

  /**
   * Sets the value of the 'Longitude' field.
   * @param value the value to set.
   */
  public void setLongitude(CharSequence value) {
    this.Longitude = value;
  }

  /**
   * Creates a new cityAttributes RecordBuilder.
   * @return A new cityAttributes RecordBuilder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Creates a new cityAttributes RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new cityAttributes RecordBuilder
   */
  public static Builder newBuilder(Builder other) {
    return new Builder(other);
  }

  /**
   * Creates a new cityAttributes RecordBuilder by copying an existing cityAttributes instance.
   * @param other The existing instance to copy.
   * @return A new cityAttributes RecordBuilder
   */
  public static Builder newBuilder(cityAttributes other) {
    return new Builder(other);
  }

  /**
   * RecordBuilder for cityAttributes instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<cityAttributes>
    implements org.apache.avro.data.RecordBuilder<cityAttributes> {

    private CharSequence city;
    private CharSequence Latitude;
    private CharSequence Longitude;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.city)) {
        this.city = data().deepCopy(fields()[0].schema(), other.city);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.Latitude)) {
        this.Latitude = data().deepCopy(fields()[1].schema(), other.Latitude);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.Longitude)) {
        this.Longitude = data().deepCopy(fields()[2].schema(), other.Longitude);
        fieldSetFlags()[2] = true;
      }
    }

    /**
     * Creates a Builder by copying an existing cityAttributes instance
     * @param other The existing instance to copy.
     */
    private Builder(cityAttributes other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.city)) {
        this.city = data().deepCopy(fields()[0].schema(), other.city);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.Latitude)) {
        this.Latitude = data().deepCopy(fields()[1].schema(), other.Latitude);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.Longitude)) {
        this.Longitude = data().deepCopy(fields()[2].schema(), other.Longitude);
        fieldSetFlags()[2] = true;
      }
    }

    /**
      * Gets the value of the 'city' field.
      * @return The value.
      */
    public CharSequence getCity() {
      return city;
    }

    /**
      * Sets the value of the 'city' field.
      * @param value The value of 'city'.
      * @return This builder.
      */
    public Builder setCity(CharSequence value) {
      validate(fields()[0], value);
      this.city = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'city' field has been set.
      * @return True if the 'city' field has been set, false otherwise.
      */
    public boolean hasCity() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'city' field.
      * @return This builder.
      */
    public Builder clearCity() {
      city = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'Latitude' field.
      * @return The value.
      */
    public CharSequence getLatitude() {
      return Latitude;
    }

    /**
      * Sets the value of the 'Latitude' field.
      * @param value The value of 'Latitude'.
      * @return This builder.
      */
    public Builder setLatitude(CharSequence value) {
      validate(fields()[1], value);
      this.Latitude = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'Latitude' field has been set.
      * @return True if the 'Latitude' field has been set, false otherwise.
      */
    public boolean hasLatitude() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'Latitude' field.
      * @return This builder.
      */
    public Builder clearLatitude() {
      Latitude = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'Longitude' field.
      * @return The value.
      */
    public CharSequence getLongitude() {
      return Longitude;
    }

    /**
      * Sets the value of the 'Longitude' field.
      * @param value The value of 'Longitude'.
      * @return This builder.
      */
    public Builder setLongitude(CharSequence value) {
      validate(fields()[2], value);
      this.Longitude = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'Longitude' field has been set.
      * @return True if the 'Longitude' field has been set, false otherwise.
      */
    public boolean hasLongitude() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'Longitude' field.
      * @return This builder.
      */
    public Builder clearLongitude() {
      Longitude = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public cityAttributes build() {
      try {
        cityAttributes record = new cityAttributes();
        record.city = fieldSetFlags()[0] ? this.city : (CharSequence) defaultValue(fields()[0]);
        record.Latitude = fieldSetFlags()[1] ? this.Latitude : (CharSequence) defaultValue(fields()[1]);
        record.Longitude = fieldSetFlags()[2] ? this.Longitude : (CharSequence) defaultValue(fields()[2]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<cityAttributes>
    WRITER$ = (org.apache.avro.io.DatumWriter<cityAttributes>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<cityAttributes>
    READER$ = (org.apache.avro.io.DatumReader<cityAttributes>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}