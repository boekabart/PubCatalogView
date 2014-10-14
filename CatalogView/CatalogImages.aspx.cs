using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using Newtonsoft.Json;

namespace CatalogView
{
    public partial class CatalogImages : System.Web.UI.Page
    {
        protected void Page_Load(object sender, EventArgs e)
        {
            var wc = new WebClient { Encoding = Encoding.UTF8 };
            var ro = JsonConvert.DeserializeObject<RootObject>(wc.DownloadString("http://localhost/traxis/web/categories/props/Name,ParentCategories,Pictures?output=json"));
            Category.SetAll(ro.Categories.Category);
            //ro.Categories.Category.Where( c => c.Pictures!=null && c.Pictures.Picture.Any()).Dump();
            var sets = ro.Categories.Category
             .Select(c => new { Cat = c.id, Name = c.Path, Pics = c.Pictures == null ? null : c.Pictures.Picture.Select(p => p.Value).FirstOrDefault() })
             .ToArray();
            var linesA = sets
             .Where(p => p.Pics != null)
             .OrderBy(p => p.Name)
                //	 .Select( p => new { p.Cat, p.Name, p.Pics, Size = GetSize(p.Pics) })
             .Select(p => string.Format("<tr><td><img src='{2}?h=96' /></td><td><img src='{2}' height='96' /></td><td><b>{1}</b><br/>{0}</td></tr>", p.Cat, p.Name, p.Pics));
            var linesB = sets
             .Where(p => p.Pics == null)
             .OrderBy(p => p.Name)
                //	 .Select( p => new { p.Cat, p.Name, p.Pics, Size = GetSize(p.Pics) })
             .Select(p => string.Format("<tr><td>n/a</td><td>n/a</td><td><b>{1}</b><br/>{0}</td></tr>", p.Cat, p.Name));

            var lines = new[] { "<html><body><table><tr><th>POSTERIS?W=96</th><th>POSTERIS RAW</th><th>Path &amp; Id</th></tr>" }
            .Concat(linesA)
            .Concat(linesB)
            .Concat(new[] { "</table></body></html>" });

        	 Response.Write(string.Join("\n", lines));
        }
    }
    public class ParentCategories
    {
        public int resultCount { get; set; }
        public List<Category> Category { get; set; }
    }

    public class Picture
    {
        public string Value { get; set; }
        public string type { get; set; }
    }

    public class Pictures
    {
        public List<Picture> Picture { get; set; }
    }

    public class Category
    {
        public static void SetAll(IEnumerable<Category> src) { foreach (var c in src) _dic[c.id] = c; }
        public static Category Find(string id) { return _dic[id]; }
        private static Dictionary<string, Category> _dic = new Dictionary<string, Category>();
        public string id { get; set; }
        public string Name { get; set; }
        public ParentCategories ParentCategories { get; set; }
        public Pictures Pictures { get; set; }
        public Category Parent { get { return ParentCategories == null ? null : Find(ParentCategories.Category.Single().id); } }
        public string Path { get { var par = Parent; if (par == null) return Name; return par.Path + " > " + Name; } }
    }

    public class Categories
    {
        public int resultCount { get; set; }
        public List<Category> Category { get; set; }
    }

    public class RootObject
    {
        public Categories Categories { get; set; }
    }
}