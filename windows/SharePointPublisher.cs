using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SharePoint.Client;
using Naveego.Pipeline;
using Naveego.Pipeline.Protocol;
using Naveego.Pipeline.Publishers;
using Naveego.Pipeline.Publishers.Transport;

namespace SharePointPublisher
{
    public class SharePointPublisher : AbstractPublisher
    {

        public static string[] SUPPORTED_FIELD_TYPES = new string[] { "Text", "Note", "Number", "Integer", "Guid", "Boolean" };

        public static Dictionary<string, string> FIELDNAME_SPECIAL_CHARS = new Dictionary<string, string>
        {
            { "~", "_x007e_" },
            { "!", "_x0021_" },
            { "@", "_x0040_" },
            { "#", "_x0023_" },
            { "$", "_x0024_" },
            { "%", "_x0025_" },
            { "^", "_x005e_" },
            { "&", "_x0026_" },
            { "*", "_x002a_" },
            { "(", "_x0028_" },
            { ")", "_x0029_" },
            { "+", "_x002b_" },
            { "-", "_x002d_" },
            { "=", "_x003d_" },
            { "{", "_x007b_" },
            { "}", "_x007d_" },
            { ":", "_x003a_" },
            { "\"", "_x005c_" },
            { "<", "_x003c_" },
            { ">", "_x003e_" },
            { "?", "_x003f_" },
            { ",", "_x002c_" },
            { ".", "_x002e_" },
            { "/", "_x002f_" },
            { "`", "_x0060_" },
            { " ", "_x0020_" }
        };

        private string _siteUrl;
        private string _username;
        private string _password;
        private string _domain;
        private IList<ShapeDefinition> _shapes;

        public override InitializeResponse Init(InitializePublisherRequest request)
        {
            _siteUrl = request.Settings["site_url"] as string;
            _username = request.Settings["username"] as string;
            _password = request.Settings["password"] as string;
            _domain = request.Settings["domain"] as string;
            

            if (_shapes == null)
            {
                _shapes = Shapes(new DiscoverPublisherShapesRequest { Settings = request.Settings }).Shapes;
            }

            return new InitializeResponse { Success = true };
        }

        public override DiscoverShapesResponse Shapes(DiscoverPublisherShapesRequest request)
        {
            var siteUrl = request.Settings["site_url"] as string;
            var userName = request.Settings["username"] as string;
            var password = request.Settings["password"] as string;
            var domain = request.Settings["domain"] as string;
            var shapes = new List<ShapeDefinition>();

            ClientContext clientContext = new ClientContext(siteUrl);
            clientContext.Credentials = new System.Net.NetworkCredential(userName, password, domain);
            Web site = clientContext.Web;

            clientContext.Load(site);
            clientContext.ExecuteQuery();

            clientContext.Load(site.Lists);
            clientContext.ExecuteQuery();


            foreach (var list in site.Lists)
            {
                if (list.BaseType == BaseType.GenericList)
                {
                    var shape = new ShapeDefinition
                    {
                        Name = list.Title,
                        Description = list.Description,
                        Properties = new List<PropertyDefinition>()
                    };

                    clientContext.Load(list.Fields);
                    clientContext.ExecuteQuery();

                    foreach (var field in list.Fields)
                    {
                        if (SUPPORTED_FIELD_TYPES.Contains(field.TypeAsString))
                        {
                            var propDef = new PropertyDefinition
                            {
                                Name = normalizeFieldName(field.Title),
                                Description = field.Description
                            };

                            switch(field.FieldTypeKind)
                            {
                                case FieldType.Boolean:
                                    propDef.Type = "boolean";
                                    break;
                                case FieldType.Number:
                                case FieldType.Integer:
                                    propDef.Type = "number";
                                    break;
                                default:
                                    propDef.Type = "string";
                                    break;
                            }

                            shape.Properties.Add(propDef);
                        }
                    }

                    shapes.Add(shape);
                }
            }

            _shapes = shapes;
            return new DiscoverShapesResponse { Shapes = shapes };
        }

        public override PublishResponse Publish(PublishRequest request, IDataTransport dataTransport)
        {

            var shapeToPublish = _shapes.FirstOrDefault(s => s.Name == request.ShapeName);

            ClientContext clientContext = new ClientContext(_siteUrl);
            clientContext.Credentials = new System.Net.NetworkCredential(_username, _password, _domain);
            Web site = clientContext.Web;

            clientContext.Load(site);
            clientContext.ExecuteQuery();

            var list = site.Lists.GetByTitle(request.ShapeName);

            CamlQuery query = new CamlQuery();
            query.ViewXml = "<View><Query></Query></View>";
            var items = list.GetItems(query);

            clientContext.Load(items);
            clientContext.ExecuteQuery();

            foreach(var item in items)
            {
                var dataPoint = new DataPoint();
                dataPoint.Action = DataPointAction.Upsert;
                dataPoint.Entity = request.ShapeName;
                dataPoint.Data = new Dictionary<string, object>();

                foreach(var prop in shapeToPublish.Properties)
                {
                    var fieldName = prepareFieldName(prop.Name);
                    if (item.FieldValues.ContainsKey(fieldName))
                    {
                        dataPoint.Data[prop.Name] = item.FieldValues[fieldName];
                    }
                    else
                    {
                        dataPoint.Data[prop.Name] = null;
                    }
                }

                dataTransport.Send(new List<DataPoint>(new [] { dataPoint }));
            }

            return new PublishResponse { Success = true };
        }

        private string normalizeFieldName(string fieldName)
        {
            if (fieldName.EndsWith("_0"))
            {
                return fieldName.Substring(0, fieldName.Length - 2);
            }

            return fieldName;
        }

        private string prepareFieldName(string fieldName)
        {
            foreach (var kvp in FIELDNAME_SPECIAL_CHARS)
            {
                fieldName = fieldName.Replace(kvp.Key, kvp.Value);
            }

            return fieldName;
        }
    }
}
