using System.Diagnostics;
using System.Text.Json;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using OpenTelemetry.Trace;
using OrderBeans.HealthChecks;
using OrderBeans.Messaging;
using OrderBeans.Models;
using Prometheus;

var builder = WebApplication.CreateBuilder(args);

// otel-tracing plan Step 2/Q4: surfaces the active trace/span ID in log scopes
// (Console:IncludeScopes in appsettings.json renders them) so a trace can be
// cross-referenced against kubectl logs / Loki without a UI click-through.
builder.Logging.Configure(options =>
{
    options.ActivityTrackingOptions = ActivityTrackingOptions.TraceId
        | ActivityTrackingOptions.SpanId
        | ActivityTrackingOptions.ParentId;
});

// otel-tracing plan Step 2: ASP.NET Core + HttpClient auto-instrumentation covers
// the inbound request and (once BeanApp is instrumented in Step 4) the hop into
// this service for free. AddSource("OrderBeans") picks up the manual publish-side
// span started in EventPublisher.cs -- OTEL_SERVICE_NAME/OTEL_EXPORTER_OTLP_ENDPOINT
// are set as env vars in deploy/deployment.yaml, read automatically by AddOtlpExporter.
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing
        .AddAspNetCoreInstrumentation()
        .AddHttpClientInstrumentation()
        .AddSource("OrderBeans")
        .AddOtlpExporter());

builder.Services.Configure<RabbitMqOptions>(builder.Configuration.GetSection(RabbitMqOptions.SectionName));
builder.Services.AddSingleton<EventPublisher>();

builder.Services
    .AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
    .AddJwtBearer(options =>
    {
        options.Authority = builder.Configuration["Keycloak:Authority"];
        options.RequireHttpsMetadata = false; // Keycloak runs on plain HTTP inside the cluster (no TLS hardening, per the architecture plan's non-goals)
        options.TokenValidationParameters.ValidateAudience = true;
        options.TokenValidationParameters.ValidAudience = "phaseolus-internal-api";
    });
builder.Services.AddAuthorization();

builder.Services.AddHealthChecks()
    .AddCheck("self", () => Microsoft.Extensions.Diagnostics.HealthChecks.HealthCheckResult.Healthy(), tags: ["live"])
    .AddCheck<RabbitMqHealthCheck>("rabbitmq", tags: ["ready"]);

var app = builder.Build();

app.UseAuthentication();
app.UseAuthorization();

app.UseHttpMetrics();
app.MapMetrics();

app.MapHealthChecks("/healthz/live", new() { Predicate = c => c.Tags.Contains("live") });
app.MapHealthChecks("/healthz/ready", new() { Predicate = c => c.Tags.Contains("ready") });

var ordersPlaced = Metrics.CreateCounter("orders_placed_total", "Total number of orders placed.");

app.MapPost("/orders", (CreateOrderRequest request, EventPublisher publisher, ILogger<Program> logger) =>
{
    var orderId = Guid.NewGuid();
    var payload = new OrderCreatedPayload(request.Items, request.TotalAmount, request.Email, request.DeliveryNote);

    var envelope = new EventEnvelope(
        EventId: Guid.NewGuid(),
        EventType: "order.created",
        OccurredAt: DateTimeOffset.UtcNow,
        OrderId: orderId,
        Payload: JsonSerializer.SerializeToElement(payload));

    publisher.Publish("order.created", envelope);
    ordersPlaced.Inc();
    logger.LogInformation("Order {OrderId} placed ({ItemCount} item(s), total {TotalAmount})",
        orderId, request.Items.Count, request.TotalAmount);

    return Results.Accepted(value: new { orderId });
}).RequireAuthorization();

app.Run();
