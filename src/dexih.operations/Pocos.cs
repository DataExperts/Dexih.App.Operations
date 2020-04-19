using System;
using dexih.repository;

namespace dexih.operations
{
    public class ObjectTypeKey: IEquatable<ObjectTypeKey>
    {
    public ESharedObjectType ObjectType { get; set; }
    public long ObjectKey { get; set; }

    public bool Equals(ObjectTypeKey other)
    {
        if (ReferenceEquals(null, other)) return false;
        if (ReferenceEquals(this, other)) return true;
        return ObjectType == other.ObjectType && ObjectKey == other.ObjectKey;
    }

    public override bool Equals(object obj)
    {
        if (ReferenceEquals(null, obj)) return false;
        if (ReferenceEquals(this, obj)) return true;
        if (obj.GetType() != this.GetType()) return false;
        return Equals((ObjectTypeKey) obj);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine((int) ObjectType, ObjectKey);
    }
    }
}