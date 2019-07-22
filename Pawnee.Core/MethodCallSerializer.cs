using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;

namespace Pawnee.Core
{
    public class ReflectedMethodCall
    {
        public string Type { get; set; }
        public string Method { get; set; }
        public List<string> Args { get; set; }
    }

    public interface IMethodCallSerializer
    {
        string Capture<T>(Expression<Func<T, Task>> expression);

        Task Invoke(string json, params object[] extraServices);
    }

    public class MethodCallSerializer : IMethodCallSerializer
    {
        private readonly IServiceProvider _services;

        public MethodCallSerializer(IServiceProvider services)
        {
            _services = services;
        }

        public string Capture<T>(Expression<Func<T, Task>> expression)
        {
            var call = (MethodCallExpression)expression.Body;

            var type = call.Method.DeclaringType;
            var method = call.Method;

            return JsonConvert.SerializeObject(new ReflectedMethodCall
            {
                Type = type.AssemblyQualifiedName,
                Method = method.Name,
                Args = (from a in call.Arguments
                        let c = a as ConstantExpression
                        let v = c != null
                            ? c.Value
                            : Expression.Lambda(a).Compile().DynamicInvoke()
                        select JsonConvert.SerializeObject(v)).ToList()
            });
        }

        private object GetTarget(Type type, object[] extraServices)
        {
            if (type.IsClass) return ActivatorUtilities.CreateInstance(_services, type, extraServices);

            var target = _services.GetService(type);

            if (target == null) return extraServices.Single(s => type.IsInstanceOfType(s));
            
            return target;
        }

        public Task Invoke(string json, params object[] extraServices)
        {
            var call = JsonConvert.DeserializeObject<ReflectedMethodCall>(json);

            var type = Type.GetType(call.Type, true);
            var method = type.GetMethod(call.Method);
            var instance = GetTarget(type, extraServices);
            var parameters = method.GetParameters();

            var args = Enumerable.Range(0, parameters.Length)
                        .Select(p => JsonConvert.DeserializeObject(
                            call.Args[p], 
                            parameters[p].ParameterType))
                        .ToArray();

            return (Task)method.Invoke(instance, args);
        }
    }
}
