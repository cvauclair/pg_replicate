use reqwest::StatusCode;

use crate::test_app::{
    spawn_app, CreateTenantRequest, CreateTenantResponse, TenantResponse, TestApp,
    UpdateTenantRequest,
};

pub async fn create_tenant(app: &TestApp) -> i64 {
    create_tenant_with_name(app, "NewTenant".to_string()).await
}

pub async fn create_tenant_with_name(app: &TestApp, name: String) -> i64 {
    let tenant = CreateTenantRequest { name };
    let response = app.create_tenant(&tenant).await;
    let response: CreateTenantResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    response.id
}

#[tokio::test]
async fn tenant_can_be_created() {
    // Arrange
    let app = spawn_app().await;

    // Act
    let tenant = CreateTenantRequest {
        name: "NewTenant".to_string(),
    };
    let response = app.create_tenant(&tenant).await;

    // Assert
    assert!(response.status().is_success());
    let response: CreateTenantResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, 1);

    let tenant_id = response.id;
    let response = app.read_tenant(tenant_id).await;
    let response: TenantResponse = response
        .json()
        .await
        .expect("failed to deserialize response");

    assert_eq!(response.id, tenant_id);
    assert_eq!(response.name, tenant.name);
}

#[tokio::test]
async fn an_existing_tenant_can_be_read() {
    // Arrange
    let app = spawn_app().await;
    let tenant = CreateTenantRequest {
        name: "NewTenant".to_string(),
    };
    let response = app.create_tenant(&tenant).await;
    let response: CreateTenantResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let tenant_id = response.id;

    // Act
    let response = app.read_tenant(tenant_id).await;

    // Assert
    assert!(response.status().is_success());
    let response: TenantResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, tenant_id);
    assert_eq!(response.name, tenant.name);
}

#[tokio::test]
async fn an_non_existing_tenant_cant_be_read() {
    // Arrange
    let app = spawn_app().await;

    // Act
    let response = app.read_tenant(42).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn an_existing_tenant_can_be_updated() {
    // Arrange
    let app = spawn_app().await;
    let tenant = CreateTenantRequest {
        name: "NewTenant".to_string(),
    };
    let response = app.create_tenant(&tenant).await;
    let response: CreateTenantResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let tenant_id = response.id;

    // Act
    let updated_tenant = UpdateTenantRequest {
        name: "UpdatedTenant".to_string(),
    };
    let response = app.update_tenant(tenant_id, &updated_tenant).await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_tenant(tenant_id).await;
    let response: TenantResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, tenant_id);
    assert_eq!(response.name, updated_tenant.name);
}

#[tokio::test]
async fn an_non_existing_tenant_cant_be_updated() {
    // Arrange
    let app = spawn_app().await;

    // Act
    let updated_tenant = UpdateTenantRequest {
        name: "UpdatedTenant".to_string(),
    };
    let response = app.update_tenant(42, &updated_tenant).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn an_existing_tenant_can_be_deleted() {
    // Arrange
    let app = spawn_app().await;
    let tenant = CreateTenantRequest {
        name: "NewTenant".to_string(),
    };
    let response = app.create_tenant(&tenant).await;
    let response: CreateTenantResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let tenant_id = response.id;

    // Act
    let response = app.delete_tenant(tenant_id).await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_tenant(tenant_id).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn an_non_existing_tenant_cant_be_deleted() {
    // Arrange
    let app = spawn_app().await;

    // Act
    let response = app.delete_tenant(42).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn all_tenants_can_be_read() {
    // Arrange
    let app = spawn_app().await;
    let tenant1_id = create_tenant_with_name(&app, "Tenant1".to_string()).await;
    let tenant2_id = create_tenant_with_name(&app, "Tenant2".to_string()).await;

    // Act
    let response = app.read_all_tenants().await;

    // Assert
    assert!(response.status().is_success());
    let response: Vec<TenantResponse> = response
        .json()
        .await
        .expect("failed to deserialize response");
    for tenant in response {
        if tenant.id == tenant1_id {
            assert_eq!(tenant.name, "Tenant1");
        } else if tenant.id == tenant2_id {
            assert_eq!(tenant.name, "Tenant2");
        }
    }
}
