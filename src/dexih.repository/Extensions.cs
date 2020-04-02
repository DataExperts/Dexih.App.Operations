using System;
using dexih.functions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace dexih.repository
{
    public static class Extensions
    {
        public static PropertyBuilder<T> HasJsonConversion<T>(this PropertyBuilder<T> propertyBuilder)
        {           
            ValueConverter<T, String> converter = new ValueConverter<T, String>(
                v => v.Serialize(),
                v => v.Deserialize<T>(true));

            ValueComparer<T> comparer = new ValueComparer<T>(
                (l, r) => l.Serialize() == r.Serialize(),
                v => v == null ? 0 : v.Serialize().GetHashCode(),
                v => v.Serialize().Deserialize<T>(true));

            propertyBuilder.HasConversion(converter);
            propertyBuilder.Metadata.SetValueConverter(converter);
            propertyBuilder.Metadata.SetValueComparer(comparer);            

            return propertyBuilder;
        }  
    }
}