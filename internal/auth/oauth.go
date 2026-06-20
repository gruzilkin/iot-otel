package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/github"
)

// Provider abstracts the OAuth2 login flow so the callback handler can be tested
// against a stub and a non-GitHub OIDC provider can be slotted in later.
type Provider interface {
	AuthCodeURL(state string) string
	Exchange(ctx context.Context, code string) (*oauth2.Token, error)
	UserID(ctx context.Context, tok *oauth2.Token) (int64, error)
}

// GitHubProvider maps a GitHub login to the numeric user id stored in
// devices.user_id (GitHub's principal id, matching the legacy Spring behaviour).
type GitHubProvider struct {
	cfg     *oauth2.Config
	userURL string
}

func NewGitHubProvider(clientID, clientSecret, redirectURL string) *GitHubProvider {
	return &GitHubProvider{
		cfg: &oauth2.Config{
			ClientID:     clientID,
			ClientSecret: clientSecret,
			RedirectURL:  redirectURL,
			Endpoint:     github.Endpoint,
			Scopes:       []string{"read:user"},
		},
		userURL: "https://api.github.com/user",
	}
}

func (p *GitHubProvider) AuthCodeURL(state string) string { return p.cfg.AuthCodeURL(state) }

func (p *GitHubProvider) Exchange(ctx context.Context, code string) (*oauth2.Token, error) {
	return p.cfg.Exchange(ctx, code)
}

func (p *GitHubProvider) UserID(ctx context.Context, tok *oauth2.Token) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.userURL, nil)
	if err != nil {
		return 0, err
	}
	resp, err := p.cfg.Client(ctx, tok).Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("github user endpoint: %s", resp.Status)
	}
	var body struct {
		ID int64 `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0, err
	}
	if body.ID == 0 {
		return 0, fmt.Errorf("github user id missing")
	}
	return body.ID, nil
}
