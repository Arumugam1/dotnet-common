namespace automation.components.data.v1.API
{
    public interface IRequest
    {
        string ID { get; set; }
        string Tenant { get; set; }
        bool IsAdmin();
    }
}
